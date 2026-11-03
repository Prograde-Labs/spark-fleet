<script setup>
import { computed, onMounted, ref } from 'vue'
import {
  Activity,
  Boxes,
  CheckCircle2,
  Cpu,
  FileText,
  Network,
  Pencil,
  Plus,
  RefreshCw,
  Server,
  Trash2,
  Wifi,
  X,
} from 'lucide-vue-next'
import { api } from '../api'
import { formatBytes, formatPercent, formatRelative, statusLabel, statusTone } from '../format'
import LogStream from '../components/LogStream.vue'

const nodes = ref([])
const checks = ref({})
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const deleting = ref(null)
const formOpen = ref(false)
const formMode = ref('create')
const formBusy = ref(false)
const formError = ref(null)
const form = ref({
  id: '',
  displayName: '',
  lanIp: '',
  fabricIp: '',
  agentUrl: '',
  enabled: true,
})
const detailOpen = ref(false)
const selected = ref(null)
const containers = ref([])
const containersBusy = ref(false)
const containersError = ref(null)
const logTarget = ref(null)

const sorted = computed(() =>
  [...nodes.value].sort((a, b) => a.id.localeCompare(b.id)),
)

function healthFor(node) {
  return checks.value[node.id] || null
}

function healthTone(node) {
  if (!node.enabled) return { label: 'Disabled', tone: '' }
  const check = healthFor(node)
  if (!check) return { label: 'Unverified', tone: 'chip-warn' }
  return check.healthy
    ? { label: 'Online', tone: 'chip-live' }
    : { label: 'Unreachable', tone: 'chip-danger' }
}

async function load() {
  loading.value = true
  error.value = null
  try {
    const response = await api.nodes.list()
    nodes.value = response.nodes || []
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

function openCreate() {
  formMode.value = 'create'
  form.value = {
    id: '',
    displayName: '',
    lanIp: '',
    fabricIp: '',
    agentUrl: '',
    enabled: true,
  }
  formError.value = null
  formOpen.value = true
}

function openEdit(node) {
  formMode.value = 'edit'
  form.value = {
    id: node.id,
    displayName: node.display_name,
    lanIp: node.lan_ip,
    fabricIp: node.fabric_ip,
    agentUrl: node.agent_url,
    enabled: node.enabled,
  }
  formError.value = null
  formOpen.value = true
}

async function saveNode() {
  formBusy.value = true
  formError.value = null
  try {
    if (formMode.value === 'create') {
      await api.nodes.create(form.value)
      notice.value = `Registered ${form.value.id}`
    } else {
      await api.nodes.update(form.value.id, {
        displayName: form.value.displayName,
        lanIp: form.value.lanIp,
        fabricIp: form.value.fabricIp,
        agentUrl: form.value.agentUrl,
        enabled: form.value.enabled,
      })
      notice.value = `Updated ${form.value.id}`
    }
    formOpen.value = false
    await load()
    if (selected.value?.id === form.value.id) {
      selected.value = nodes.value.find((node) => node.id === form.value.id) || null
    }
  } catch (exc) {
    formError.value = exc.message || String(exc)
  } finally {
    formBusy.value = false
  }
}

async function checkNode(node) {
  try {
    checks.value = { ...checks.value, [node.id]: await api.nodes.check(node.id) }
    await load()
    if (selected.value?.id === node.id) {
      selected.value = nodes.value.find((item) => item.id === node.id) || selected.value
    }
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

async function deleteNode(node) {
  if (deleting.value !== node.id) {
    deleting.value = node.id
    return
  }
  deleting.value = null
  try {
    await api.nodes.remove(node.id)
    notice.value = `Removed ${node.id}`
    if (selected.value?.id === node.id) detailOpen.value = false
    const nextChecks = { ...checks.value }
    delete nextChecks[node.id]
    checks.value = nextChecks
    await load()
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

async function openDetail(node) {
  selected.value = node
  detailOpen.value = true
  await loadContainers()
}

async function loadContainers() {
  if (!selected.value) return
  containersBusy.value = true
  containersError.value = null
  try {
    const response = await api.nodes.containers(selected.value.id)
    containers.value = response.containers || []
  } catch (exc) {
    containersError.value = exc.message || String(exc)
    containers.value = []
  } finally {
    containersBusy.value = false
  }
}

function openLogs(container) {
  logTarget.value = {
    container,
    url: api.nodes.containerLogStreamUrl(selected.value.id, container.id),
  }
}

function containerLabel(container, key) {
  return container.labels?.[key] || '—'
}

onMounted(load)
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Fleet Registry</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Node Agents</h3>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <button class="btn" :disabled="loading" @click="load">
          <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': loading }" />
          Refresh
        </button>
        <button class="btn btn-primary" @click="openCreate">
          <Plus class="h-3.5 w-3.5" />
          Register node
        </button>
      </div>
    </header>

    <div
      v-if="error"
      class="flex items-center gap-3 rounded-lg border border-danger/40 bg-danger/10 px-4 py-3"
    >
      <p class="font-mono text-xs text-danger">{{ error }}</p>
      <button class="btn ml-auto" @click="error = null">Dismiss</button>
    </div>
    <div
      v-else-if="notice"
      class="flex items-center gap-3 rounded-lg border border-signal/30 bg-signal/[0.05] px-4 py-3"
    >
      <p class="font-mono text-xs text-signal">{{ notice }}</p>
      <button class="btn ml-auto" @click="notice = null">Dismiss</button>
    </div>

    <div v-if="loading && !nodes.length" class="panel grid place-items-center p-12">
      <RefreshCw class="h-5 w-5 animate-spin text-cyan" />
      <p class="mt-2 font-mono text-[11px] text-faint">Loading node registry…</p>
    </div>

    <div v-else class="grid gap-4 xl:grid-cols-2">
      <article
        v-for="node in sorted"
        :key="node.id"
        class="panel relative overflow-hidden p-5"
      >
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-cyan/30 to-transparent" />
        <header class="flex items-start gap-3">
          <div class="grid h-9 w-9 place-items-center rounded-md border border-edge bg-void/60">
            <Server class="h-4 w-4 text-cyan" />
          </div>
          <div class="min-w-0 flex-1">
            <div class="flex flex-wrap items-center gap-2">
              <h3 class="truncate text-sm font-semibold">{{ node.display_name }}</h3>
              <span class="chip" :class="healthTone(node).tone">
                <span class="h-1.5 w-1.5 rounded-full bg-current" />
                {{ healthTone(node).label }}
              </span>
              <span v-if="node.enabled" class="chip">enabled</span>
              <span v-else class="chip">disabled</span>
            </div>
            <p class="mt-0.5 font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
              {{ node.id }} · {{ node.agent_url }}
            </p>
          </div>
        </header>

        <div class="mt-4 grid gap-2 sm:grid-cols-2">
          <div class="rounded-md border border-edge/70 bg-void/40 px-3 py-2">
            <p class="hud-label flex items-center gap-1.5"><Wifi class="h-3 w-3" /> LAN</p>
            <p class="hud-value mt-0.5 text-[12px]">{{ node.lan_ip }}</p>
          </div>
          <div class="rounded-md border border-edge/70 bg-void/40 px-3 py-2">
            <p class="hud-label flex items-center gap-1.5"><Network class="h-3 w-3" /> Fabric</p>
            <p class="hud-value mt-0.5 text-[12px]">{{ node.fabric_ip }}</p>
          </div>
          <div class="rounded-md border border-edge/70 bg-void/40 px-3 py-2 sm:col-span-2">
            <p class="hud-label flex items-center gap-1.5"><Cpu class="h-3 w-3" /> Agent</p>
            <p class="mt-0.5 font-mono text-[11px] text-dim">
              {{ node.info?.hostname || 'unknown host' }} ·
              {{ node.info?.version || 'no version' }} ·
              {{ node.info?.platform || 'platform pending' }}
            </p>
          </div>
        </div>

        <div class="mt-3 flex flex-wrap items-center gap-2">
          <span class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
            Seen {{ formatRelative(node.last_seen_at) }}
          </span>
          <span v-if="healthFor(node)?.error" class="chip chip-danger">
            {{ healthFor(node).error }}
          </span>
          <div class="ml-auto flex items-center gap-1.5">
            <button class="btn" @click="checkNode(node)">
              <Activity class="h-3.5 w-3.5" />
              Ping
            </button>
            <button class="btn" @click="openDetail(node)">
              <Boxes class="h-3.5 w-3.5" />
              Inspect
            </button>
            <button class="btn" @click="openEdit(node)">
              <Pencil class="h-3.5 w-3.5" />
            </button>
            <button
              class="btn"
              :class="deleting === node.id ? 'btn-danger' : ''"
              @click="deleteNode(node)"
            >
              <Trash2 class="h-3.5 w-3.5" />
              {{ deleting === node.id ? 'Confirm' : '' }}
            </button>
          </div>
        </div>
      </article>

      <div v-if="!nodes.length" class="panel col-span-full grid place-items-center p-12 text-center">
        <Server class="h-6 w-6 text-faint" />
        <p class="mt-2 text-sm text-dim">No nodes registered.</p>
        <button class="btn btn-primary mt-3" @click="openCreate">Register the first node</button>
      </div>
    </div>

    <!-- Node form -->
    <div
      v-if="formOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="formOpen = false"
    >
      <div class="panel w-full max-w-lg p-6">
        <div class="flex items-center gap-2">
          <Server class="h-4 w-4 text-cyan" />
          <h3 class="text-base font-semibold">
            {{ formMode === 'create' ? 'Register Node' : 'Edit Node' }}
          </h3>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="formOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <div class="divider my-4" />
        <div class="grid gap-3 sm:grid-cols-2">
          <div>
            <label class="hud-label">Node id</label>
            <input
              v-model="form.id"
              class="input mt-1"
              :disabled="formMode === 'edit'"
              placeholder="spark-01"
            />
          </div>
          <div>
            <label class="hud-label">Display name</label>
            <input v-model="form.displayName" class="input mt-1" placeholder="Spark 01" />
          </div>
          <div>
            <label class="hud-label">LAN IP</label>
            <input v-model="form.lanIp" class="input mt-1" placeholder="192.0.2.239" />
          </div>
          <div>
            <label class="hud-label">Fabric IP</label>
            <input v-model="form.fabricIp" class="input mt-1" placeholder="198.51.100.10" />
          </div>
          <div class="sm:col-span-2">
            <label class="hud-label">Agent URL</label>
            <input v-model="form.agentUrl" class="input mt-1" placeholder="http://192.0.2.239:8092" />
          </div>
          <label class="flex items-center gap-2 font-mono text-[11px] text-dim sm:col-span-2">
            <input v-model="form.enabled" type="checkbox" class="accent-cyan" />
            Poll this node for health and metrics
          </label>
        </div>
        <p v-if="formError" class="mt-3 font-mono text-[11px] text-danger">{{ formError }}</p>
        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="formOpen = false">Cancel</button>
          <button class="btn btn-primary" :disabled="formBusy" @click="saveNode">
            <CheckCircle2 class="h-3.5 w-3.5" />
            {{ formMode === 'create' ? 'Register' : 'Save' }}
          </button>
        </div>
      </div>
    </div>

    <!-- Node detail -->
    <div
      v-if="detailOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="detailOpen = false"
    >
      <div class="panel max-h-[88vh] w-full max-w-4xl overflow-y-auto p-6">
        <div class="flex items-center gap-2">
          <Server class="h-4 w-4 text-cyan" />
          <div>
            <h3 class="text-base font-semibold">{{ selected?.display_name }}</h3>
            <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
              {{ selected?.id }} · {{ selected?.agent_url }}
            </p>
          </div>
          <span class="chip ml-2" :class="healthTone(selected || {}).tone">
            {{ healthTone(selected || {}).label }}
          </span>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="detailOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <div class="divider my-4" />

        <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <div class="panel-raised p-3">
            <p class="hud-label">Hostname</p>
            <p class="hud-value mt-1 text-[12px]">{{ selected?.info?.hostname || '—' }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Agent version</p>
            <p class="hud-value mt-1 text-[12px]">{{ selected?.info?.version || '—' }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Platform</p>
            <p class="mt-1 font-mono text-[11px] text-dim">{{ selected?.info?.platform || '—' }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Data dir</p>
            <p class="mt-1 font-mono text-[11px] text-dim">{{ selected?.info?.data_dir || '—' }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Compose dir</p>
            <p class="mt-1 font-mono text-[11px] text-dim">{{ selected?.info?.compose_projects_dir || '—' }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Cache roots</p>
            <p class="mt-1 font-mono text-[11px] text-dim">
              {{ (selected?.info?.model_cache_roots || []).join(', ') || '—' }}
            </p>
          </div>
        </div>

        <div class="mt-5 flex items-center gap-2">
          <Boxes class="h-4 w-4 text-violet" />
          <h4 class="text-sm font-semibold">Managed Containers</h4>
          <span class="chip ml-2">{{ containers.length }}</span>
          <button class="btn ml-auto" :disabled="containersBusy" @click="loadContainers">
            <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': containersBusy }" />
            Refresh
          </button>
        </div>

        <p v-if="containersError" class="mt-2 font-mono text-[11px] text-danger">
          {{ containersError }}
        </p>

        <div class="mt-3 space-y-2">
          <div
            v-for="container in containers"
            :key="container.id"
            class="flex flex-wrap items-center gap-2 rounded-md border border-edge/70 bg-void/40 px-3 py-2"
          >
            <div class="min-w-0 flex-1">
              <p class="truncate font-mono text-[11px] text-ink">{{ container.name }}</p>
              <p class="font-mono text-[9px] uppercase tracking-[0.12em] text-faint">
                {{ containerLabel(container, 'spark.fleet.deployment') }} /
                {{ containerLabel(container, 'spark.fleet.role') }} ·
                {{ container.id?.slice(0, 12) }}
              </p>
            </div>
            <span class="chip" :class="statusTone(container.state)">
              {{ statusLabel(container.state) }}
            </span>
            <span class="font-mono text-[10px] text-dim">
              {{ formatBytes(container.memory_usage) }}
              <span v-if="container.memory_percent !== null">
                · {{ formatPercent(container.memory_percent, 1) }}
              </span>
            </span>
            <button
              class="rounded-md border border-edge p-1 text-faint transition hover:border-cyan/40 hover:text-cyan"
              title="Live logs"
              @click="openLogs(container)"
            >
              <FileText class="h-3.5 w-3.5" />
            </button>
          </div>
          <p
            v-if="!containers.length && !containersBusy"
            class="rounded-md border border-dashed border-edge px-3 py-4 text-center font-mono text-[10px] text-faint"
          >
            No managed containers on this node.
          </p>
        </div>
      </div>
    </div>

    <!-- Live logs -->
    <div
      v-if="logTarget"
      class="fixed inset-0 z-[70] grid place-items-center bg-void/85 px-4 backdrop-blur-sm"
      @click.self="logTarget = null"
    >
      <div class="w-full max-w-4xl">
        <div class="mb-2 flex items-center gap-2">
          <p class="font-mono text-[10px] uppercase tracking-[0.16em] text-cyan-soft">
            {{ logTarget.container.name }}
          </p>
          <button class="btn ml-auto" @click="logTarget = null">
            <X class="h-3.5 w-3.5" />
            Close
          </button>
        </div>
        <LogStream
          :url="logTarget.url"
          :title="logTarget.container.name"
          :subtitle="`${selected?.id} · ${logTarget.container.id}`"
        />
      </div>
    </div>
  </div>
</template>
