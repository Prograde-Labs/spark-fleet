<script setup>
import { nextTick, onBeforeUnmount, onMounted, ref } from 'vue'
import { ArrowDown, Pause, Play, Terminal, Trash2 } from 'lucide-vue-next'

const props = defineProps({
  url: { type: String, required: true },
  title: { type: String, default: 'Container Logs' },
  subtitle: { type: String, default: '' },
  maxChars: { type: Number, default: 400_000 },
})

const text = ref('')
const following = ref(false)
const error = ref(null)
const stick = ref(true)
const terminal = ref(null)
let controller = null

async function start() {
  stop()
  error.value = null
  text.value = ''
  following.value = true
  controller = new AbortController()
  try {
    const response = await fetch(props.url, {
      credentials: 'same-origin',
      signal: controller.signal,
    })
    if (!response.ok) {
      const body = await response.text()
      let detail = body
      try {
        detail = JSON.parse(body).detail || body
      } catch {
        /* keep raw body */
      }
      error.value = detail || `Log stream failed (${response.status})`
      following.value = false
      return
    }
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      text.value += decoder.decode(value, { stream: true })
      if (text.value.length > props.maxChars) {
        text.value = text.value.slice(-props.maxChars)
      }
      await nextTick()
      if (stick.value) scrollToEnd()
    }
    text.value += decoder.decode()
  } catch (exc) {
    if (exc.name !== 'AbortError') error.value = String(exc)
  } finally {
    following.value = false
  }
}

function stop() {
  if (controller) {
    controller.abort()
    controller = null
  }
  following.value = false
}

function clear() {
  text.value = ''
}

function scrollToEnd() {
  if (!terminal.value) return
  terminal.value.scrollTop = terminal.value.scrollHeight
}

function onScroll() {
  if (!terminal.value) return
  const distance =
    terminal.value.scrollHeight - terminal.value.scrollTop - terminal.value.clientHeight
  stick.value = distance < 48
}

function jumpToLive() {
  stick.value = true
  scrollToEnd()
}

onMounted(start)
onBeforeUnmount(stop)

defineExpose({ start, stop, clear })
</script>

<template>
  <section class="overflow-hidden rounded-lg border border-edge bg-void/80">
    <header class="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-3">
      <Terminal class="h-4 w-4 text-cyan" />
      <div class="min-w-0">
        <p class="truncate text-sm font-semibold">{{ title }}</p>
        <p v-if="subtitle" class="truncate font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
          {{ subtitle }}
        </p>
      </div>
      <span class="ml-auto chip" :class="following ? 'chip-live' : 'chip-warn'">
        <span class="h-1.5 w-1.5 rounded-full bg-current" :class="{ 'animate-pulseSoft': following }" />
        {{ following ? 'streaming' : 'paused' }}
      </span>
      <button class="btn" @click="following ? stop() : start()">
        <Pause v-if="following" class="h-3.5 w-3.5" />
        <Play v-else class="h-3.5 w-3.5" />
        {{ following ? 'Pause' : 'Follow' }}
      </button>
      <button class="btn" @click="clear">
        <Trash2 class="h-3.5 w-3.5" />
        Clear
      </button>
    </header>

    <div v-if="error" class="border-b border-danger/30 bg-danger/10 px-4 py-2">
      <p class="font-mono text-[11px] text-danger">{{ error }}</p>
    </div>

    <div class="relative">
      <pre
        ref="terminal"
        class="h-[46vh] overflow-auto bg-void/60 p-4 font-mono text-[11px] leading-relaxed text-dim"
        @scroll="onScroll"
      >{{ text || 'Waiting for log output…' }}</pre>
      <button
        v-if="!stick"
        class="absolute bottom-3 right-3 flex items-center gap-1.5 rounded-md border border-cyan/40 bg-void/90 px-2.5 py-1.5 font-mono text-[10px] uppercase tracking-[0.12em] text-cyan-soft shadow-glow-cyan"
        @click="jumpToLive"
      >
        <ArrowDown class="h-3 w-3" />
        Jump to live
      </button>
    </div>
  </section>
</template>
