import { createSignal } from "solid-js"

export interface VoiceMemoMetadata {
  id: string
  name: string
  date: string
  duration: number
  mimeType: string
  trimmed: boolean
}

export interface VoiceMemo extends VoiceMemoMetadata {
  audio: Blob
  original?: Blob
  originalDuration?: number
}

interface TrimRange {
  start: number
  end: number
}

const DB_NAME = "opencode-voice-memos"
const DB_VERSION = 1
const STORE_NAME = "memos"

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION)
    request.onupgradeneeded = () => {
      const db = request.result
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        const store = db.createObjectStore(STORE_NAME, { keyPath: "id" })
        store.createIndex("date", "date", { unique: false })
      }
    }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
}

function generateId(): string {
  return `memo_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`
}

async function decodeAudio(blob: Blob): Promise<AudioBuffer> {
  const ctx = new AudioContext()
  const buffer = await blob.arrayBuffer()
  const decoded = await ctx.decodeAudioData(buffer)
  ctx.close()
  return decoded
}

async function encodeAudio(buffer: AudioBuffer, mimeType: string): Promise<Blob> {
  const offline = new OfflineAudioContext(buffer.numberOfChannels, buffer.length, buffer.sampleRate)
  const source = offline.createBufferSource()
  source.buffer = buffer
  source.connect(offline.destination)
  source.start()
  const rendered = await offline.startRendering()
  return audioBufferToWav(rendered, mimeType)
}

function audioBufferToWav(buffer: AudioBuffer, mimeType: string): Blob {
  const channels = buffer.numberOfChannels
  const length = buffer.length
  const rate = buffer.sampleRate
  const bitsPerSample = 16
  const byteRate = (rate * channels * bitsPerSample) / 8
  const blockAlign = (channels * bitsPerSample) / 8
  const dataSize = length * blockAlign
  const headerSize = 44
  const output = new ArrayBuffer(headerSize + dataSize)
  const view = new DataView(output)

  const writeString = (offset: number, str: string) => {
    for (let i = 0; i < str.length; i++) view.setUint8(offset + i, str.charCodeAt(i))
  }

  writeString(0, "RIFF")
  view.setUint32(4, 36 + dataSize, true)
  writeString(8, "WAVE")
  writeString(12, "fmt ")
  view.setUint32(16, 16, true)
  view.setUint16(20, 1, true)
  view.setUint16(22, channels, true)
  view.setUint32(24, rate, true)
  view.setUint32(28, byteRate, true)
  view.setUint16(32, blockAlign, true)
  view.setUint16(34, bitsPerSample, true)
  writeString(36, "data")
  view.setUint32(40, dataSize, true)

  let offset = headerSize
  for (let i = 0; i < length; i++) {
    for (let ch = 0; ch < channels; ch++) {
      const sample = Math.max(-1, Math.min(1, buffer.getChannelData(ch)[i]))
      view.setInt16(offset, sample < 0 ? sample * 0x8000 : sample * 0x7fff, true)
      offset += 2
    }
  }

  return new Blob([output], { type: mimeType || "audio/wav" })
}

export function createVoiceMemoStore() {
  const [memos, setMemos] = createSignal<VoiceMemoMetadata[]>([])
  const [recording, setRecording] = createSignal(false)

  let recorder: MediaRecorder | undefined
  let chunks: Blob[] = []

  async function save(memo: VoiceMemo): Promise<void> {
    const db = await openDB()
    return new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readwrite")
      tx.objectStore(STORE_NAME).put(memo)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
  }

  async function load(id: string): Promise<VoiceMemo | undefined> {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readonly")
      const request = tx.objectStore(STORE_NAME).get(id)
      request.onsuccess = () => resolve(request.result ?? undefined)
      request.onerror = () => reject(request.error)
    })
  }

  async function list(): Promise<VoiceMemoMetadata[]> {
    const db = await openDB()
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readonly")
      const request = tx.objectStore(STORE_NAME).index("date").openCursor(null, "prev")
      const results: VoiceMemoMetadata[] = []
      request.onsuccess = () => {
        const cursor = request.result
        if (cursor) {
          const val = cursor.value as VoiceMemo
          results.push({
            id: val.id,
            name: val.name,
            date: val.date,
            duration: val.duration,
            mimeType: val.mimeType,
            trimmed: val.trimmed,
          })
          cursor.continue()
        } else {
          resolve(results)
        }
      }
      request.onerror = () => reject(request.error)
    })
  }

  async function refresh(): Promise<VoiceMemoMetadata[]> {
    const items = await list()
    setMemos(items)
    return items
  }

  async function startRecording(name?: string): Promise<void> {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
    const mimeType = MediaRecorder.isTypeSupported("audio/webm;codecs=opus") ? "audio/webm;codecs=opus" : "audio/webm"
    recorder = new MediaRecorder(stream, { mimeType })
    chunks = []

    recorder.ondataavailable = (e) => {
      if (e.data.size > 0) chunks.push(e.data)
    }

    recorder.onstop = async () => {
      stream.getTracks().forEach((t) => t.stop())
      const blob = new Blob(chunks, { type: mimeType })
      const id = generateId()
      const memo: VoiceMemo = {
        id,
        name: name || `Voice Memo ${new Date().toLocaleString()}`,
        date: new Date().toISOString(),
        duration: 0,
        mimeType,
        trimmed: false,
        audio: blob,
      }

      const audio = new Audio(URL.createObjectURL(blob))
      await new Promise<void>((resolve) => {
        audio.onloadedmetadata = () => {
          memo.duration = audio.duration
          resolve()
        }
        audio.onerror = () => resolve()
      })

      await save(memo)
      await refresh()
      setRecording(false)
    }

    recorder.start()
    setRecording(true)
  }

  function stopRecording(): void {
    if (recorder && recorder.state !== "inactive") {
      recorder.stop()
    }
  }

  async function trim(id: string, range: TrimRange): Promise<void> {
    const memo = await load(id)
    if (!memo) return
    const decoded = await decodeAudio(memo.audio)
    const rate = decoded.sampleRate
    const startSample = Math.max(0, Math.floor(range.start * rate))
    const endSample = Math.min(decoded.length, Math.floor(range.end * rate))
    if (startSample >= endSample) return

    const length = endSample - startSample
    const offline = new OfflineAudioContext(decoded.numberOfChannels, length, rate)
    const trimmed = offline.createBuffer(decoded.numberOfChannels, length, rate)

    for (let ch = 0; ch < decoded.numberOfChannels; ch++) {
      const src = decoded.getChannelData(ch)
      const dst = trimmed.getChannelData(ch)
      for (let i = 0; i < length; i++) {
        dst[i] = src[startSample + i]
      }
    }

    const blob = await encodeAudio(trimmed, memo.mimeType)

    const updated: VoiceMemo = {
      ...memo,
      original: memo.original || memo.audio,
      originalDuration: memo.originalDuration ?? memo.duration,
      audio: blob,
      duration: trimmed.duration,
      trimmed: true,
    }

    await save(updated)
    await refresh()
  }

  async function restoreOriginal(id: string): Promise<boolean> {
    const memo = await load(id)
    if (!memo || !memo.original) return false

    const restored: VoiceMemo = {
      ...memo,
      audio: memo.original,
      duration: memo.originalDuration ?? memo.duration,
      original: undefined,
      originalDuration: undefined,
      trimmed: false,
    }

    await save(restored)
    await refresh()
    return true
  }

  async function hasOriginal(id: string): Promise<boolean> {
    const memo = await load(id)
    return !!memo?.original
  }

  async function remove(id: string): Promise<void> {
    const db = await openDB()
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE_NAME, "readwrite")
      tx.objectStore(STORE_NAME).delete(id)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
    await refresh()
  }

  async function get(id: string): Promise<VoiceMemo | undefined> {
    return load(id)
  }

  return {
    memos,
    recording,
    startRecording,
    stopRecording,
    trim,
    restoreOriginal,
    hasOriginal,
    remove,
    get,
    refresh,
  }
}
