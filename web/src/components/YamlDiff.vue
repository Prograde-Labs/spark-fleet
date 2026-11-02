<script setup>
import { computed } from 'vue'

const props = defineProps({
  diff: { type: String, default: '' },
})

const lines = computed(() =>
  String(props.diff || '')
    .split('\n')
    .filter((line, index, all) => !(line === '' && index === all.length - 1))
    .map((line) => {
      if (line.startsWith('+++') || line.startsWith('---')) {
        return { line, tone: 'text-faint' }
      }
      if (line.startsWith('@@')) return { line, tone: 'text-cyan' }
      if (line.startsWith('+')) return { line, tone: 'text-signal bg-signal/[0.06]' }
      if (line.startsWith('-')) return { line, tone: 'text-danger bg-danger/[0.06]' }
      return { line, tone: 'text-dim' }
    }),
)
</script>

<template>
  <div class="overflow-hidden rounded-lg border border-edge bg-void/70">
    <div class="flex items-center gap-2 border-b border-edge px-3 py-2">
      <span class="h-2 w-2 rounded-full bg-danger/70" />
      <span class="h-2 w-2 rounded-full bg-warning/70" />
      <span class="h-2 w-2 rounded-full bg-signal/70" />
      <span class="ml-2 hud-label">Recipe diff</span>
    </div>
    <div class="max-h-[52vh] overflow-auto p-3">
      <pre v-if="lines.length" class="font-mono text-[11px] leading-relaxed"><code><span
        v-for="(entry, index) in lines"
        :key="index"
        class="block whitespace-pre-wrap"
        :class="entry.tone"
      >{{ entry.line }}</span></code></pre>
      <p v-else class="font-mono text-[11px] text-faint">No differences.</p>
    </div>
  </div>
</template>
