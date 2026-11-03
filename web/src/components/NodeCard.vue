<script setup>
import { computed } from 'vue'
import {
  Activity,
  Cpu,
  HardDrive,
  MemoryStick,
  Radio,
  RefreshCw,
  Server,
  Thermometer,
  Zap,
} from 'lucide-vue-next'
import { formatBytes, formatPercent, formatRelative } from '../format'
import {
  cacheCapacityKnown,
  cachePercent,
  gpuMemoryKnown,
  gpuResident,
  groupGpus,
} from '../metrics'

const props = defineProps({
  node: { type: Object, required: true },
  metrics: { type: Object, default: () => ({}) },
  health: { type: Object, default: null },
  deployments: { type: Array, default: () => [] },
})

const emit = defineEmits(['check'])

const RADIUS = 42
const CIRCUMFERENCE = 2 * Math.PI * RADIUS

const total = computed(() => Number(props.metrics.host_total_bytes || 0))
const available = computed(() => Number(props.metrics.host_available_bytes || 0))
const used = computed(() => Math.max(0, total.value - available.value))
const memoryPercent = computed(() =>
  total.value > 0 ? Math.min(100, (used.value / total.value) * 100) : 0,
)
const ringOffset = computed(
  () => CIRCUMFERENCE * (1 - memoryPercent.value / 100),
)
const cacheTotal = computed(() => props.metrics.cache_total_bytes)
const cacheFree = computed(() => props.metrics.cache_free_bytes)
const cacheKnown = computed(
  () =>
    cacheCapacityKnown(props.metrics) &&
    cacheFree.value !== null &&
    cacheFree.value !== undefined,
)
const cacheBarPercent = computed(() => cachePercent(props.metrics))
const containerPercent = computed(() =>
  total.value > 0
    ? Math.min(100, (Number(props.metrics.container_memory_bytes || 0) / total.value) * 100)
    : 0,
)

const gpus = computed(() => groupGpus(props.metrics))

const online = computed(() => {
  if (!props.node.enabled) return { label: 'Disabled', tone: '' }
  if (props.health && typeof props.health.healthy === 'boolean') {
    return props.health.healthy
      ? { label: 'Online', tone: 'chip-live' }
      : { label: 'Unreachable', tone: 'chip-danger' }
  }
  return { label: 'Unverified', tone: 'chip-warn' }
})

const loaded = computed(() =>
  props.deployments.filter((item) => item.deployment.status === 'loaded'),
)

const gpuPercent = (gpu, key) => {
  const totalKey = 'memory_total_bytes'
  const usedKey = 'memory_used_bytes'
  if (key === 'memory') {
    if (!gpu[totalKey]) return 0
    return Math.min(100, ((gpu[usedKey] || 0) / gpu[totalKey]) * 100)
  }
  return Math.min(100, Number(gpu[key] || 0))
}
</script>

<template>
  <article class="panel group relative overflow-hidden p-5 transition hover:border-edgeBright">
    <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-cyan/30 to-transparent" />

    <header class="flex items-start gap-3">
      <div class="grid h-9 w-9 place-items-center rounded-md border border-edge bg-void/60">
        <Server class="h-4 w-4 text-cyan" />
      </div>
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2">
          <h3 class="truncate text-sm font-semibold tracking-tight">{{ node.display_name }}</h3>
          <span class="chip" :class="online.tone">
            <span class="h-1.5 w-1.5 rounded-full bg-current" />
            {{ online.label }}
          </span>
        </div>
        <p class="mt-0.5 font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
          {{ node.id }} · {{ node.lan_ip }}
        </p>
      </div>
      <button
        class="rounded-md border border-edge p-1.5 text-faint transition hover:border-cyan/40 hover:text-cyan"
        title="Ping agent"
        @click="emit('check', node)"
      >
        <RefreshCw class="h-3.5 w-3.5" />
      </button>
    </header>

    <div class="mt-5 grid grid-cols-[112px_1fr] items-center gap-4">
      <div class="relative grid h-[112px] w-[112px] place-items-center">
        <svg viewBox="0 0 112 112" class="absolute inset-0 -rotate-90">
          <circle cx="56" cy="56" :r="RADIUS" fill="none" stroke="#1d2637" stroke-width="7" />
          <circle
            cx="56"
            cy="56"
            :r="RADIUS"
            fill="none"
            stroke="url(#memGradient)"
            stroke-width="7"
            stroke-linecap="round"
            :stroke-dasharray="CIRCUMFERENCE"
            :stroke-dashoffset="ringOffset"
            class="transition-all duration-700"
          />
          <defs>
            <linearGradient id="memGradient" x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stop-color="#22d3ee" />
              <stop offset="100%" stop-color="#a78bfa" />
            </linearGradient>
          </defs>
        </svg>
        <div class="text-center">
          <p class="font-mono text-xl font-semibold tabular-nums">
            {{ formatPercent(memoryPercent) }}
          </p>
          <p class="hud-label">Unified mem</p>
        </div>
      </div>

      <div class="space-y-3">
        <div>
          <div class="flex items-center justify-between">
            <span class="hud-label flex items-center gap-1.5">
              <MemoryStick class="h-3 w-3" /> Used
            </span>
            <span class="hud-value text-[11px]">{{ formatBytes(used) }}</span>
          </div>
          <div class="mt-1 h-1.5 overflow-hidden rounded-full bg-edge">
            <div
              class="h-full rounded-full bg-gradient-to-r from-cyan to-violet transition-all duration-700"
              :style="{ width: `${memoryPercent}%` }"
            />
          </div>
          <p class="mt-1 font-mono text-[9px] text-faint">
            {{ formatBytes(available) }} free of {{ formatBytes(total) }}
          </p>
        </div>

        <div>
          <div class="flex items-center justify-between">
            <span class="hud-label flex items-center gap-1.5">
              <HardDrive class="h-3 w-3" /> Cache
            </span>
            <span class="hud-value text-[11px]">{{ formatBytes(metrics.cache_bytes) }}</span>
          </div>
          <div class="mt-1 h-1.5 overflow-hidden rounded-full bg-edge">
            <div
              class="h-full rounded-full bg-violet/70 transition-all duration-700"
              :style="{ width: `${cacheBarPercent}%` }"
            />
          </div>
          <p class="mt-1 font-mono text-[9px] text-faint">
            <template v-if="cacheKnown">
              Filesystem {{ formatBytes(cacheFree) }} free of {{ formatBytes(cacheTotal) }}
            </template>
            <template v-else>Capacity unavailable</template>
          </p>
          <p class="mt-0.5 font-mono text-[9px] text-faint">
            Container {{ formatBytes(metrics.container_memory_bytes) }} ·
            {{ formatPercent(containerPercent, 1) }} of host
          </p>
        </div>
      </div>
    </div>

    <div v-if="gpus.length" class="mt-4 space-y-2">
      <div
        v-for="gpu in gpus"
        :key="gpu.index"
        class="rounded-md border border-edge/70 bg-void/40 px-3 py-2"
      >
        <div class="flex items-center justify-between">
          <span class="hud-label flex items-center gap-1.5">
            <Cpu class="h-3 w-3" /> GPU {{ gpu.index }}
          </span>
          <span class="flex items-center gap-3 font-mono text-[10px] text-dim">
            <span class="flex items-center gap-1">
              <Zap class="h-3 w-3 text-cyan" />
              {{
                gpu.utilization_percent === null || gpu.utilization_percent === undefined
                  ? 'N/A'
                  : formatPercent(gpu.utilization_percent)
              }}
            </span>
            <span class="flex items-center gap-1">
              <Thermometer class="h-3 w-3 text-warning" />
              {{
                gpu.temperature_celsius === null || gpu.temperature_celsius === undefined
                  ? 'N/A'
                  : `${gpu.temperature_celsius}°C`
              }}
            </span>
            <span
              v-if="gpuResident(gpu) !== null"
              class="chip"
              :class="gpuResident(gpu) ? 'chip-live' : ''"
            >
              <span class="h-1.5 w-1.5 rounded-full bg-current" />
              {{ gpuResident(gpu) ? 'resident' : 'idle' }}
            </span>
          </span>
        </div>
        <div class="mt-1.5 flex items-center gap-2">
          <template v-if="gpuMemoryKnown(gpu)">
            <div class="h-1 flex-1 overflow-hidden rounded-full bg-edge">
              <div
                class="h-full rounded-full bg-cyan/70"
                :style="{ width: `${gpuPercent(gpu, 'memory')}%` }"
              />
            </div>
            <span class="w-24 text-right font-mono text-[9px] text-faint">
              {{ formatBytes(gpu.memory_used_bytes) }} / {{ formatBytes(gpu.memory_total_bytes) }}
            </span>
          </template>
          <span v-else class="flex-1 font-mono text-[9px] text-faint">
            GPU memory N/A
          </span>
        </div>
        <p
          v-if="
            (gpu.compute_memory_bytes !== null && gpu.compute_memory_bytes !== undefined) ||
            (gpu.compute_processes !== null && gpu.compute_processes !== undefined)
          "
          class="mt-1 font-mono text-[9px] text-faint"
        >
          <template v-if="gpu.compute_memory_bytes !== null && gpu.compute_memory_bytes !== undefined">
            {{ formatBytes(gpu.compute_memory_bytes) }} in compute apps
          </template>
          <template v-if="gpu.compute_processes !== null && gpu.compute_processes !== undefined">
            <span v-if="gpu.compute_memory_bytes !== null && gpu.compute_memory_bytes !== undefined"> · </span>
            {{ gpu.compute_processes }}
            process{{ Number(gpu.compute_processes) === 1 ? '' : 'es' }}
          </template>
        </p>
      </div>
    </div>
    <div v-else class="mt-4 rounded-md border border-dashed border-edge px-3 py-2">
      <p class="hud-label flex items-center gap-1.5">
        <Activity class="h-3 w-3" /> No GPU telemetry
      </p>
    </div>

    <div class="mt-4 flex flex-wrap items-center gap-2">
      <span
        v-for="item in loaded"
        :key="item.deployment.id"
        class="chip"
        :class="item.deployment.is_primary ? 'chip-live' : ''"
      >
        <Radio class="h-3 w-3" />
        {{ item.deployment.name }}
      </span>
      <span v-if="!loaded.length" class="hud-label">No active workload</span>
    </div>

    <footer class="mt-4 flex items-center justify-between border-t border-edge/70 pt-3">
      <span class="font-mono text-[9px] uppercase tracking-[0.16em] text-faint">
        Fabric {{ node.fabric_ip }}
      </span>
      <span class="font-mono text-[9px] text-faint">
        Seen {{ formatRelative(node.last_seen_at) }}
      </span>
    </footer>
  </article>
</template>
