<script setup>
defineProps({
  title: { type: String, required: true },
  code: { type: String, default: 'MODULE' },
  note: { type: String, default: 'Awaiting telemetry link.' },
})
</script>

<template>
  <section class="panel relative overflow-hidden p-8">
    <div class="absolute inset-0 bg-grid-fade opacity-30 mask-fade-b" aria-hidden="true" />
    <div class="relative">
      <p class="hud-label text-cyan/70">{{ code }}</p>
      <h3 class="mt-2 text-xl font-semibold tracking-tight">{{ title }}</h3>
      <p class="mt-2 max-w-lg text-sm text-dim">{{ note }}</p>
      <div class="mt-6 flex items-center gap-2">
        <span class="h-1.5 w-1.5 animate-pulseSoft rounded-full bg-cyan" />
        <span class="font-mono text-[10px] uppercase tracking-[0.18em] text-faint">
          Module offline
        </span>
      </div>
    </div>
  </section>
</template>
