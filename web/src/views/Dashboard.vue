<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  Activity,
  Boxes,
  Database,
  RefreshCw,
  Radio,
  Server,
  Star,
  Terminal,
  Wifi,
  WifiOff,
} from 'lucide-vue-next'
import { api } from '../api'
import {
  formatBytes,
  formatRelative,
  formatTime,
  statusLabel,
  statusTone,
} from '../format'
import { eventState } from '../stores/events'
import { cacheAggregate } from '../metrics'
import NodeCard from '../components/NodeCard.vue'
import Topology from '../components/Topology.vue'

const nodes = ref([])
const deployments = ref([])
const metricsByNode = ref({})
const nodeChecks = ref({})
const loading = ref(true)
const refreshing = ref(false)
const error = ref(null)
const notice = ref(null)
const busyPrimary = ref(null)
const now = ref(Date.now())

let timer = null
let ticker = null

const loadedDeployments = computed(() =>
  deployments.value.filter((item) => item.deployment.status === 'loaded'),
)
const primaryCandidates = computed(() =>
  loadedDeployments.value.filter((item) => item.deployment.mode === 'deployment'),
)
const primaryDeployment = computed(
  () => primaryCandidates.value.find((item) => item.deployment.is_primary) || null,
)
const cacheTotals = computed(() => cacheAggregate(metricsByNode.value))
const onlineNodes = computed(
  () =>
    nodes.value.filter((node) => {
      const health = eventState.nodes[node.id] || nodeChecks.value[node.id]
      return node.enabled && (!health || health.healthy !== false)
    }).length,
)
const liveEvents = computed(() => eventState.events.slice(0, 40))

async function load() {
  refreshing.value = true
  error.value = null
  try {
    const [nodeResponse, deploymentResponse] = await Promise.all([
      api.nodes.list(),
      api.deployments.list(),
    ])
    nodes.value = nodeResponse.nodes || []
    deployments.value = deploymentResponse.deployments || []
    await loadMetrics()
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
    refreshing.value = false
  }
}

async function loadMetrics() {
  const entries = await Promise.all(
    nodes.value.map(async (node) => {
      try {
        const response = await api.metrics.node(node.id)
        return [node.id, response.latest || {}]
      } catch {
        return [node.id, metricsByNode.value[node.id] || {}]
      }
    }),
  )
  metricsByNode.value = Object.fromEntries(entries)
}

async function checkNode(node) {
  try {
    nodeChecks.value = {
      ...nodeChecks.value,
      [node.id]: await api.nodes.check(node.id),
    }
  } catch (exc) {
    notice.value = `${node.display_name}: ${exc.message}`
  }
}

async function makePrimary(item) {
  if (item.deployment.is_primary) return
  busyPrimary.value = item.deployment.id
  notice.value = null
  try {
    await api.deployments.setPrimary(item.deployment.id)
    notice.value = `${item.deployment.name} is now spark-active`
    await load()
  } catch (exc) {
    notice.value = exc.message || String(exc)
  } finally {
    busyPrimary.value = null
  }
}

function healthFor(node) {
  return eventState.nodes[node.id] || nodeChecks.value[node.id] || null
}

function eventMessage(event) {
  const data = event.data || {}
  switch (event.type) {
    case 'job':
      return `${data.kind || 'job'} · ${statusLabel(data.status)} · ${data.message || ''}`.trim()
    case 'node.health':
      return `${data.node_id} ${data.healthy ? 'online' : 'offline'}${
        data.error ? ` · ${data.error}` : ''
      }`
    case 'metrics.sample':
      return `${data.node_id} telemetry · ${formatBytes(data.host_available_bytes)} free`
    case 'deployment.reconciled':
      return `${data.name} ${statusLabel(data.status)}${
        data.problems?.length ? ` · ${data.problems.join('; ')}` : ''
      }`
    default:
      return typeof data === 'string' ? data : JSON.stringify(data)
  }
}

function eventTone(event) {
  if (event.type === 'node.health') {
    return event.data?.healthy ? 'text-signal' : 'text-danger'
  }
  if (event.type === 'deployment.reconciled') {
    return event.data?.status === 'failed' ? 'text-danger' : 'text-cyan'
  }
  if (event.type === 'job') {
    const status = String(event.data?.status || '')
    if (status === 'failed' || status === 'cancelled') return 'text-danger'
    if (status === 'succeeded') return 'text-signal'
    return 'text-warning'
  }
  return 'text-violet-soft'
}

onMounted(async () => {
  await load()
  timer = setInterval(load, 15000)
  ticker = setInterval(() => {
    now.value = Date.now()
  }, 1000)
})

onBeforeUnmount(() => {
  if (timer) clearInterval(timer)
  if (ticker) clearInterval(ticker)
})
</script>

<template>
  <div class="space-y-6">
    <!-- Fleet stats -->
    <section class="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <div class="panel relative overflow-hidden p-4">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-cyan/50 to-transparent" />
        <div class="flex items-center justify-between">
          <p class="hud-label">Agents online</p>
          <Wifi class="h-4 w-4 text-cyan" />
        </div>
        <p class="stat-value mt-2">{{ onlineNodes }}<span class="text-faint">/{{ nodes.length }}</span></p>
        <p class="mt-1 font-mono text-[10px] text-faint">LAN + fabric links</p>
      </div>
      <div class="panel relative overflow-hidden p-4">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-violet/50 to-transparent" />
        <div class="flex items-center justify-between">
          <p class="hud-label">Loaded deployments</p>
          <Boxes class="h-4 w-4 text-violet" />
        </div>
        <p class="stat-value mt-2">{{ loadedDeployments.length }}</p>
        <p class="mt-1 font-mono text-[10px] text-faint">
          {{ deployments.length }} registered recipes
        </p>
      </div>
      <div class="panel relative overflow-hidden p-4">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-signal/50 to-transparent" />
        <div class="flex items-center justify-between">
          <p class="hud-label">spark-active</p>
          <Star class="h-4 w-4 text-signal" />
        </div>
        <p class="mt-2 truncate text-lg font-semibold tracking-tight">
          {{ primaryDeployment?.deployment.name || 'No primary' }}
        </p>
        <p class="mt-1 font-mono text-[10px] text-faint">
          {{ primaryDeployment ? primaryDeployment.deployment.status : 'gateway idle' }}
        </p>
      </div>
      <div class="panel relative overflow-hidden p-4">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-warning/50 to-transparent" />
        <div class="flex items-center justify-between">
          <p class="hud-label">Model cache</p>
          <Database class="h-4 w-4 text-warning" />
        </div>
        <p class="stat-value mt-2">{{ formatBytes(cacheTotals.used) }}</p>
        <p class="mt-1 font-mono text-[10px] text-faint">
          <template v-if="cacheTotals.complete">
            of {{ formatBytes(cacheTotals.capacity) }} · {{ formatBytes(cacheTotals.free) }} free
          </template>
          <template v-else>Across all cache roots</template>
        </p>
      </div>
    </section>

    <div
      v-if="error"
      class="flex items-center gap-3 rounded-lg border border-danger/40 bg-danger/10 px-4 py-3"
    >
      <WifiOff class="h-4 w-4 text-danger" />
      <p class="font-mono text-xs text-danger">{{ error }}</p>
      <button class="btn ml-auto" @click="load">Retry</button>
    </div>
    <div
      v-else-if="notice"
      class="flex items-center gap-3 rounded-lg border border-cyan/30 bg-cyan/[0.06] px-4 py-3"
    >
      <Radio class="h-4 w-4 text-cyan" />
      <p class="font-mono text-xs text-cyan-soft">{{ notice }}</p>
      <button class="btn ml-auto" @click="notice = null">Dismiss</button>
    </div>

    <div class="grid gap-6 xl:grid-cols-[minmax(0,1.55fr)_minmax(0,1fr)]">
      <div class="space-y-6">
        <Topology
          :nodes="nodes"
          :deployments="deployments"
          :primary-id="primaryDeployment?.deployment.id"
          :health="eventState.nodes"
        />

        <div>
          <div class="mb-3 flex items-center justify-between">
            <div>
              <p class="hud-label text-cyan/70">Node Telemetry</p>
              <h3 class="mt-1 text-base font-semibold tracking-tight">Compute Nodes</h3>
            </div>
            <button class="btn" :disabled="refreshing" @click="load">
              <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': refreshing }" />
              Refresh
            </button>
          </div>
          <div class="grid gap-4 lg:grid-cols-2">
            <NodeCard
              v-for="node in nodes"
              :key="node.id"
              :node="node"
              :metrics="metricsByNode[node.id] || {}"
              :health="healthFor(node)"
              :deployments="deployments"
              @check="checkNode"
            />
            <div
              v-if="!nodes.length && !loading"
              class="panel col-span-full grid place-items-center p-10 text-center"
            >
              <Server class="h-6 w-6 text-faint" />
              <p class="mt-2 text-sm text-dim">No nodes registered yet.</p>
              <router-link to="/nodes" class="btn btn-primary mt-3">Register a node</router-link>
            </div>
          </div>
        </div>
      </div>

      <div class="space-y-6">
        <section class="panel p-5">
          <header class="flex items-center gap-2">
            <Boxes class="h-4 w-4 text-violet" />
            <div>
              <p class="hud-label text-violet/70">Active Fleet</p>
              <h3 class="text-base font-semibold tracking-tight">Primary Routing</h3>
            </div>
          </header>
          <div class="divider my-4" />

          <div v-if="primaryCandidates.length" class="space-y-2">
            <button
              v-for="item in primaryCandidates"
              :key="item.deployment.id"
              class="group flex w-full items-center gap-3 rounded-lg border px-3 py-2.5 text-left transition"
              :class="
                item.deployment.is_primary
                  ? 'border-signal/40 bg-signal/[0.06]'
                  : 'border-edge bg-void/40 hover:border-edgeBright'
              "
              :disabled="busyPrimary === item.deployment.id"
              @click="makePrimary(item)"
            >
              <Star
                class="h-4 w-4"
                :class="item.deployment.is_primary ? 'fill-signal text-signal' : 'text-faint'"
              />
              <div class="min-w-0 flex-1">
                <p class="truncate text-sm font-medium">{{ item.deployment.name }}</p>
                <p class="font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
                  {{ item.deployment.mode }} · {{ item.roles.length }} roles
                </p>
              </div>
              <span class="chip" :class="statusTone(item.deployment.status)">
                {{ statusLabel(item.deployment.status) }}
              </span>
            </button>
          </div>
          <p v-else class="py-6 text-center font-mono text-[11px] text-faint">
            No deployments loaded.
          </p>

          <div class="mt-4 rounded-md border border-edge bg-void/50 px-3 py-2">
            <p class="hud-label">Gateway alias</p>
            <p class="mt-1 font-mono text-xs text-cyan-soft">
              model: spark-active →
              {{ primaryDeployment?.deployment.name || 'unavailable' }}
            </p>
          </div>
        </section>

        <section class="panel overflow-hidden">
          <header class="flex items-center gap-2 px-5 pt-5">
            <Terminal class="h-4 w-4 text-cyan" />
            <div>
              <p class="hud-label text-cyan/70">Live Telemetry</p>
              <h3 class="text-base font-semibold tracking-tight">Event Rail</h3>
            </div>
            <span class="ml-auto chip" :class="eventState.connected ? 'chip-live' : 'chip-warn'">
              <Activity class="h-3 w-3" />
              {{ eventState.connected ? 'streaming' : 'standby' }}
            </span>
          </header>
          <div class="divider my-4" />
          <div class="max-h-[420px] overflow-y-auto px-5 pb-5">
            <div
              v-for="event in liveEvents"
              :key="event.id"
              class="border-b border-edge/60 py-2 last:border-0"
            >
              <div class="flex items-center gap-2">
                <span class="font-mono text-[9px] tabular-nums text-faint">
                  {{ formatTime(event.at / 1000) }}
                </span>
                <span class="font-mono text-[9px] uppercase tracking-[0.16em] text-faint">
                  {{ event.type }}
                </span>
                <span class="ml-auto font-mono text-[9px] text-faint">
                  {{ formatRelative(event.at / 1000, now) }}
                </span>
              </div>
              <p class="mt-0.5 font-mono text-[11px]" :class="eventTone(event)">
                {{ eventMessage(event) }}
              </p>
            </div>
            <p v-if="!liveEvents.length" class="py-8 text-center font-mono text-[11px] text-faint">
              Waiting for controller events…
            </p>
          </div>
        </section>
      </div>
    </div>
  </div>
</template>
