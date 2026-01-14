import { type StreamTextResult, type Tool as AITool } from "ai"
import { Identifier } from "../id/id"
import { MessageV2 } from "./message-v2"
import { Log } from "../util/log"
import { Session } from "."
import { ModelsDev } from "../provider/models"
import { Bus } from "../bus"
import { Permission } from "../permission"
import { Snapshot } from "../snapshot"
import { SessionSummary } from "./summary"

const log = Log.create({ service: "session.processor" })
const DOOM_LOOP_THRESHOLD = 3

export class SessionProcessor {
  private toolcalls: Record<string, MessageV2.ToolPart> = {}
  private assistantMsg?: MessageV2.Assistant
  private blocked = false

  constructor(
    private input: {
      sessionID: string
      providerID: string
      model: ModelsDev.Model
      abort: AbortSignal
    },
  ) {}

  get message() {
    if (!this.assistantMsg) throw new Error("call start() or set message first")
    return this.assistantMsg
  }

  partFromToolCall(toolCallID: string) {
    return this.toolcalls[toolCallID]
  }

  async start(message: MessageV2.Assistant) {
    if (this.assistantMsg) {
      throw new Error("end previous assistant message first")
    }
    this.assistantMsg = message
  }

  async end() {
    if (this.assistantMsg) {
      this.assistantMsg.time.completed = Date.now()
      await Session.updateMessage(this.assistantMsg)
      this.assistantMsg = undefined
    }
  }

  async process(
    stream: StreamTextResult<Record<string, AITool>, never>,
    retries: { count: number; max: number },
  ) {
    log.info("process")
    if (!this.assistantMsg) throw new Error("call start() first before processing")
    let shouldRetry = false
    try {
      let currentText: MessageV2.TextPart | undefined
      let reasoningMap: Record<string, MessageV2.ReasoningPart> = {}

      for await (const value of stream.fullStream) {
        this.input.abort.throwIfAborted()
        log.info("part", {
          type: value.type,
        })
        switch (value.type) {
          case "start":
            break

          case "reasoning-start":
            if (value.id in reasoningMap) {
              continue
            }
            reasoningMap[value.id] = {
              id: Identifier.ascending("part"),
              messageID: this.assistantMsg.id,
              sessionID: this.assistantMsg.sessionID,
              type: "reasoning",
              text: "",
              time: {
                start: Date.now(),
              },
              metadata: value.providerMetadata,
            }
            break

          case "reasoning-delta":
            if (value.id in reasoningMap) {
              const part = reasoningMap[value.id]
              part.text += value.text
              if (value.providerMetadata) part.metadata = value.providerMetadata
              if (part.text) await Session.updatePart({ part, delta: value.text })
            }
            break

          case "reasoning-end":
            if (value.id in reasoningMap) {
              const part = reasoningMap[value.id]
              part.text = part.text.trimEnd()

              part.time = {
                ...part.time,
                end: Date.now(),
              }
              if (value.providerMetadata) part.metadata = value.providerMetadata
              await Session.updatePart(part)
              delete reasoningMap[value.id]
            }
            break

          case "tool-input-start":
            const part = await Session.updatePart({
              id: this.toolcalls[value.id]?.id ?? Identifier.ascending("part"),
              messageID: this.assistantMsg.id,
              sessionID: this.assistantMsg.sessionID,
              type: "tool",
              tool: value.toolName,
              callID: value.id,
              state: {
                status: "pending",
                input: {},
                raw: "",
              },
            })
            this.toolcalls[value.id] = part as MessageV2.ToolPart
            break

          case "tool-input-delta":
            break

          case "tool-input-end":
            break

          case "tool-call": {
            const match = this.toolcalls[value.toolCallId]
            if (match) {
              const part = await Session.updatePart({
                ...match,
                tool: value.toolName,
                state: {
                  status: "running",
                  input: value.input,
                  time: {
                    start: Date.now(),
                  },
                },
                metadata: value.providerMetadata,
              })
              this.toolcalls[value.toolCallId] = part as MessageV2.ToolPart

              const parts = await Session.getParts(this.assistantMsg.id)
              const lastThree = parts.slice(-DOOM_LOOP_THRESHOLD)
              if (
                lastThree.length === DOOM_LOOP_THRESHOLD &&
                lastThree.every(
                  (p) =>
                    p.type === "tool" &&
                    p.tool === value.toolName &&
                    p.state.status !== "pending" &&
                    JSON.stringify(p.state.input) === JSON.stringify(value.input),
                )
              ) {
                await Permission.ask({
                  type: "doom-loop",
                  pattern: value.toolName,
                  sessionID: this.assistantMsg.sessionID,
                  messageID: this.assistantMsg.id,
                  callID: value.toolCallId,
                  title: `Possible doom loop: "${value.toolName}" called ${DOOM_LOOP_THRESHOLD} times with identical arguments`,
                  metadata: {
                    tool: value.toolName,
                    input: value.input,
                  },
                })
              }
            }
            break
          }
          case "tool-result": {
            const match = this.toolcalls[value.toolCallId]
            if (match && match.state.status === "running") {
              await Session.updatePart({
                ...match,
                state: {
                  status: "completed",
                  input: value.input,
                  output: value.output.output,
                  metadata: value.output.metadata,
                  title: value.output.title,
                  time: {
                    start: match.state.time.start,
                    end: Date.now(),
                  },
                  attachments: value.output.attachments,
                },
              })

              delete this.toolcalls[value.toolCallId]
            }
            break
          }

          case "tool-error": {
            const match = this.toolcalls[value.toolCallId]
            if (match && match.state.status === "running") {
              await Session.updatePart({
                ...match,
                state: {
                  status: "error",
                  input: value.input,
                  error: (value.error as any).toString(),
                  metadata:
                    value.error instanceof Permission.RejectedError
                      ? value.error.metadata
                      : undefined,
                  time: {
                    start: match.state.time.start,
                    end: Date.now(),
                  },
                },
              })

              if (value.error instanceof Permission.RejectedError) {
                this.blocked = true
              }
              delete this.toolcalls[value.toolCallId]
            }
            break
          }
          case "error":
            throw value.error

          case "start-step":
            this.pendingSnapshot = await Snapshot.track()
            await Session.updatePart({
              id: Identifier.ascending("part"),
              messageID: this.assistantMsg.id,
              sessionID: this.assistantMsg.sessionID,
              snapshot: this.pendingSnapshot,
              type: "step-start",
            })
            break

          case "finish-step":
            const usage = Session.getUsage({
              model: this.input.model,
              usage: value.usage,
              metadata: value.providerMetadata,
            })
            this.assistantMsg.cost += usage.cost
            this.assistantMsg.tokens = usage.tokens
            await Session.updatePart({
              id: Identifier.ascending("part"),
              reason: value.finishReason,
              snapshot: await Snapshot.track(),
              messageID: this.assistantMsg.id,
              sessionID: this.assistantMsg.sessionID,
              type: "step-finish",
              tokens: usage.tokens,
              cost: usage.cost,
            })
            await Session.updateMessage(this.assistantMsg)

            // Handling snapshot patch logic:
            // We need access to the 'snapshot' variable from start-step.
            // But start-step happens in the same loop, so we can't easily share state across events in switch unless we use a local variable in process loop.
            // Wait, in prompt.ts `snapshot` variable was scoped to `createProcessor` which was persistent across the message generation?
            // No, `snapshot` in `createProcessor` was defined outside `process`.
            // But `process` loop handles both start-step and finish-step?
            // Yes.
            // So if `start-step` sets it, `finish-step` can read it.
            // I need to add `snapshot` state to `SessionProcessor` class or `process` method?
            // In prompt.ts it was outside `process` but inside `createProcessor`.
            // Since `process` might return and be called again (retries), but `start-step` and `finish-step` usually happen within one `process` call (unless streaming is interrupted/retried).
            // Actually `start-step` and `finish-step` come from the stream.
            // If we retry, we get a new stream.
            // So `snapshot` should be local to `process` IF the step is fully contained in one stream.
            // If a step can span retries (it shouldn't), then we need it outside.
            // But `prompt.ts` had it in `createProcessor` scope.
            // Let's put it in class scope but reset it?
            // Or maybe just check if we have a pending snapshot.

            // Wait, I should implement the logic as it was.
            // In `prompt.ts`: `let snapshot: string | undefined` inside `createProcessor`.

            if (this.pendingSnapshot) {
              const patch = await Snapshot.patch(this.pendingSnapshot)
              if (patch.files.length) {
                await Session.updatePart({
                  id: Identifier.ascending("part"),
                  messageID: this.assistantMsg.id,
                  sessionID: this.assistantMsg.sessionID,
                  type: "patch",
                  hash: patch.hash,
                  files: patch.files,
                })
              }
              this.pendingSnapshot = undefined
            }
            SessionSummary.summarize({
              sessionID: this.input.sessionID,
              messageID: this.assistantMsg.parentID,
            })
            break

          case "text-start":
            currentText = {
              id: Identifier.ascending("part"),
              messageID: this.assistantMsg.id,
              sessionID: this.assistantMsg.sessionID,
              type: "text",
              text: "",
              time: {
                start: Date.now(),
              },
              metadata: value.providerMetadata,
            }
            break

          case "text-delta":
            if (currentText) {
              currentText.text += value.text
              if (value.providerMetadata) currentText.metadata = value.providerMetadata
              if (currentText.text)
                await Session.updatePart({
                  part: currentText,
                  delta: value.text,
                })
            }
            break

          case "text-end":
            if (currentText) {
              currentText.text = currentText.text.trimEnd()
              currentText.time = {
                start: Date.now(),
                end: Date.now(),
              }
              if (value.providerMetadata) currentText.metadata = value.providerMetadata
              await Session.updatePart(currentText)
            }
            currentText = undefined
            break

          case "finish":
            this.assistantMsg.time.completed = Date.now()
            await Session.updateMessage(this.assistantMsg)
            break

          default:
            log.info("unhandled", {
              ...value,
            })
            continue
        }
      }
    } catch (e) {
      log.error("process", {
        error: e,
      })
      const error = MessageV2.fromError(e, { providerID: this.input.providerID })
      if (
        retries.count < retries.max &&
        MessageV2.APIError.isInstance(error) &&
        error.data.isRetryable
      ) {
        shouldRetry = true
        await Session.updatePart({
          id: Identifier.ascending("part"),
          messageID: this.assistantMsg.id,
          sessionID: this.assistantMsg.sessionID,
          type: "retry",
          attempt: retries.count + 1,
          time: {
            created: Date.now(),
          },
          error,
        })
      } else {
        this.assistantMsg.error = error
        Bus.publish(Session.Event.Error, {
          sessionID: this.assistantMsg.sessionID,
          error: this.assistantMsg.error,
        })
      }
    }
    const p = await Session.getParts(this.assistantMsg.id)
    for (const part of p) {
      if (
        part.type === "tool" &&
        part.state.status !== "completed" &&
        part.state.status !== "error"
      ) {
        await Session.updatePart({
          ...part,
          state: {
            ...part.state,
            status: "error",
            error: "Tool execution aborted",
            time: {
              start: Date.now(),
              end: Date.now(),
            },
          },
        })
      }
    }
    if (!shouldRetry) {
      this.assistantMsg.time.completed = Date.now()
    }
    await Session.updateMessage(this.assistantMsg)
    return { info: this.assistantMsg, parts: p, blocked: this.blocked, shouldRetry }
  }

  // Internal state for snapshot tracking
  private pendingSnapshot?: string

  // We need to override the switch case for start-step to use this.pendingSnapshot
  // But I can't just inject code. I need to modify the switch case above.
  // I will rewrite the class logic to use this.pendingSnapshot correctly.
}
