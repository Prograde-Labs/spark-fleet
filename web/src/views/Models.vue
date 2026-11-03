<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  AlertTriangle,
  CheckCircle2,
  Database,
  Download,
  HardDrive,
  RefreshCw,
  Search,
  Trash2,
  X,
} from 'lucide-vue-next'
import { api } from '../api'
import { formatBytes, formatPercent, statusLabel, statusTone } from '../format'

const catalog = ref([])
const deployments = ref([])
const registryNodes = ref([])
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const search = ref('')
const downloadOpen = ref(false)
const downloadBusy = ref(false)
const downloadError = ref(null)
const downloadNodeErrors = ref([])
const downloadForm = ref({ repo: '', nodes: [], root: '', revision: '', patterns: '' })
const activeJobs = ref([])
const removeTarget = ref(null)
const cleanupTarget = ref(null)
const cleanupBusy = ref(false)

let refreshTimer = null
let jobTimer = null
let jobInFlight = false

const inUseRepos = computed(() => {
  const repos = new Set()
  for (const item of deployments.value) {
    if (item.deployment.status !== 'loaded') continue
    const model = item.deployment.recipe?.spec?.model
    if (!model) continue
    if (model.repo) repos.add(model.repo)
    for (const extra of model.extraRepos || []) repos.add(extra)
  }
  return repos
})

const nodeEntries = computed(() => {
  const term = search.value.trim().toLowerCase()
  return catalog.value.map((entry) => {
    const models = (entry.models?.models || []).filter(
      (model) => !term || model.repo.toLowerCase().includes(term),
    )
    return { ...entry, filtered: models }
  })
})

const totals = computed(() => {
  const cachedRepos = new Set()
  const partialRepos = new Set()
  const sizes = new Map()
  for (const entry of catalog.value) {
    for (const model of entry.models?.models || []) {
      if (model.cached) cachedRepos.add(model.repo)
      if (model.partial) partialRepos.add(model.repo)
      sizes.set(model.repo, Math.max(sizes.get(model.repo) || 0, Number(model.size_bytes || 0)))
    }
  }
  const bytes = [...sizes.values()].reduce((sum, value) => sum + value, 0)
  return { cached: cachedRepos.size, partial: partialRepos.size, bytes }
})

const activeCount = computed(
  () =>
    activeJobs.value.filter((job) =>
      ['pending', 'running'].includes(job.state),
    ).length,
)

const nodeName = (nodeId) =>
  registryNodes.value.find((node) => node.id === nodeId)?.display_name || nodeId

async function load() {
  error.value = null
  try {
    const [modelResponse, deploymentResponse, nodeResponse] = await Promise.all([
      api.models.list(),
      api.deployments.list(),
      api.nodes.list(),
    ])
    catalog.value = modelResponse.nodes || []
    deployments.value = deploymentResponse.deployments || []
    registryNodes.value = nodeResponse.nodes || []
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

function openDownload(repo = '', nodeId = null) {
  downloadForm.value = {
    repo,
    nodes: nodeId ? [nodeId] : [],
    root: '',
    revision: '',
    patterns: '',
  }
  activeJobs.value = []
  downloadNodeErrors.value = []
  downloadError.value = null
  downloadOpen.value = true
}

function toggleNode(nodeId) {
  const current = new Set(downloadForm.value.nodes)
  if (current.has(nodeId)) current.delete(nodeId)
  else current.add(nodeId)
  downloadForm.value.nodes = [...current]
}

async function submitDownload() {
  downloadBusy.value = true
  downloadError.value = null
  try {
    const patterns = downloadForm.value.patterns
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
    const response = await api.models.download({
      repo: downloadForm.value.repo.trim(),
      nodes: downloadForm.value.nodes,
      root: downloadForm.value.root.trim() || undefined,
      revision: downloadForm.value.revision.trim() || undefined,
      allowPatterns: patterns.length ? patterns : undefined,
    })
    downloadNodeErrors.value = response.errors || []
    activeJobs.value = (response.jobs || []).map((job) => ({
      node_id: job.node_id,
      id: job.job_id,
      state: 'pending',
      phase: 'queued',
      files_done: 0,
      files_total: null,
      bytes_done: 0,
      bytes_total: null,
      misses: 0,
    }))
    startJobPolling()
  } catch (exc) {
    downloadError.value = exc.message || String(exc)
  } finally {
    downloadBusy.value = false
  }
}

async function pollJobs() {
  if (jobInFlight) return
  jobInFlight = true
  try {
    const response = await api.models.jobs()
    const byKey = new Map(
      activeJobs.value.map((job) => [`${job.node_id}:${job.id}`, job]),
    )
    const returned = new Set()
    for (const node of response.nodes || []) {
      if (node.error) {
        for (const [key, job] of byKey) {
          if (job.node_id === node.node_id && ['pending', 'running'].includes(job.state)) {
            byKey.set(key, { ...job, state: 'failed', error: node.error })
          }
        }
        continue
      }
      for (const job of node.jobs || []) {
        const key = `${node.node_id}:${job.id}`
        returned.add(key)
        if (byKey.has(key)) byKey.set(key, { ...job, node_id: node.node_id, misses: 0 })
      }
    }
    for (const [key, job] of byKey) {
      if (!returned.has(key) && ['pending', 'running'].includes(job.state)) {
        const misses = (job.misses || 0) + 1
        byKey.set(
          key,
          misses >= 3
            ? { ...job, state: 'failed', error: 'job lost (agent restart?)', misses }
            : { ...job, misses },
        )
      }
    }
    activeJobs.value = [...byKey.values()]
    if (!activeCount.value) {
      jobTimer = null
      await load()
      return
    }
  } catch (exc) {
    downloadError.value = exc.message || String(exc)
    jobTimer = null
    return
  } finally {
    jobInFlight = false
  }
  if (activeCount.value) {
    clearTimeout(jobTimer)
    jobTimer = setTimeout(pollJobs, 2000)
  }
}

function startJobPolling() {
  if (jobTimer) clearTimeout(jobTimer)
  jobTimer = setTimeout(pollJobs, 1200)
}

async function cancelJob(job) {
  try {
    await api.models.cancelJob(job.node_id, job.id)
    await pollJobs()
  } catch (exc) {
    downloadError.value = exc.message || String(exc)
  }
}

function askRemove(entry, model) {
  removeTarget.value = { nodeId: entry.node_id, model }
}

async function confirmRemove() {
  if (!removeTarget.value) return
  const { nodeId, model } = removeTarget.value
  try {
    await api.models.remove(model.repo, { nodeId, root: model.root })
    notice.value = `Removed ${model.repo} from ${nodeName(nodeId)}`
    removeTarget.value = null
    await load()
  } catch (exc) {
    error.value = exc.message || String(exc)
    removeTarget.value = null
  }
}

function askCleanup(entry) {
  const partials = (entry.models?.models || []).filter(
    (model) => model.partial && !model.cached && !inUseRepos.value.has(model.repo),
  )
  if (!partials.length) {
    notice.value = 'No orphaned partial downloads on this node.'
    return
  }
  cleanupTarget.value = { nodeId: entry.node_id, models: partials }
}

async function confirmCleanup() {
  if (!cleanupTarget.value) return
  cleanupBusy.value = true
  const { nodeId, models } = cleanupTarget.value
  const failures = []
  for (const model of models) {
    try {
      await api.models.remove(model.repo, { nodeId, root: model.root })
    } catch (exc) {
      failures.push(`${model.repo}: ${exc.message}`)
    }
  }
  cleanupBusy.value = false
  cleanupTarget.value = null
  await load()
  notice.value = failures.length
    ? `Cleanup finished with ${failures.length} failure(s)`
    : `Purged ${models.length} partial download(s) from ${nodeName(nodeId)}`
}

function jobPercent(job) {
  if (job.bytes_total) {
    return Math.min(100, (Number(job.bytes_done || 0) / Number(job.bytes_total)) * 100)
  }
  if (job.files_total) {
    return Math.min(100, (Number(job.files_done || 0) / Number(job.files_total)) * 100)
  }
  return 0
}

onMounted(async () => {
  await load()
  refreshTimer = setInterval(load, 30000)
})

onBeforeUnmount(() => {
  if (refreshTimer) clearInterval(refreshTimer)
  if (jobTimer) clearTimeout(jobTimer)
})
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Model Cache</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Model Vault</h3>
      </div>
      <div class="ml-auto flex flex-wrap items-center gap-2">
        <div class="relative">
          <Search class="pointer-events-none absolute left-2.5 top-2.5 h-3.5 w-3.5 text-faint" />
          <input v-model="search" class="input w-56 pl-8" placeholder="Filter repo…" />
        </div>
        <button class="btn" :disabled="loading" @click="load">
          <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': loading }" />
          Scan
        </button>
        <button class="btn btn-primary" @click="openDownload()">
          <Download class="h-3.5 w-3.5" />
          Download model
        </button>
      </div>
    </header>

    <section class="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <div class="panel p-4">
        <p class="hud-label">Cached repos</p>
        <p class="stat-value mt-2">{{ totals.cached }}</p>
      </div>
      <div class="panel p-4">
        <p class="hud-label">Partial / orphaned</p>
        <p class="stat-value mt-2" :class="totals.partial ? 'text-warning' : ''">
          {{ totals.partial }}
        </p>
      </div>
      <div class="panel p-4">
        <p class="hud-label">In use</p>
        <p class="stat-value mt-2">{{ inUseRepos.size }}</p>
      </div>
      <div class="panel p-4">
        <p class="hud-label">Cache footprint</p>
        <p class="stat-value mt-2">{{ formatBytes(totals.bytes) }}</p>
      </div>
    </section>

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

    <div v-if="loading && !catalog.length" class="panel grid place-items-center p-12">
      <RefreshCw class="h-5 w-5 animate-spin text-cyan" />
      <p class="mt-2 font-mono text-[11px] text-faint">Scanning node caches…</p>
    </div>

    <div v-else class="grid gap-4 xl:grid-cols-2">
      <section v-for="entry in nodeEntries" :key="entry.node_id" class="panel overflow-hidden">
        <header class="flex items-center gap-2 border-b border-edge px-4 py-3">
          <Database class="h-4 w-4 text-cyan" />
          <div class="min-w-0">
            <p class="truncate text-sm font-semibold">{{ nodeName(entry.node_id) }}</p>
            <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
              {{ entry.node_id }} · {{ entry.models?.roots?.length || 0 }} cache roots
            </p>
          </div>
          <span v-if="entry.error" class="chip chip-danger ml-auto">agent error</span>
          <button
            v-else
            class="btn ml-auto"
            title="Remove partial downloads"
            @click="askCleanup(entry)"
          >
            <AlertTriangle class="h-3.5 w-3.5" />
            Purge partials
          </button>
        </header>

        <div v-if="entry.error" class="px-4 py-6">
          <p class="font-mono text-[11px] text-danger">{{ entry.error }}</p>
        </div>

        <div v-else class="px-4 py-3">
          <div class="flex flex-wrap gap-1.5">
            <span
              v-for="root in entry.models?.roots || []"
              :key="root.path"
              class="chip"
              :class="root.exists ? '' : 'chip-warn'"
            >
              <HardDrive class="h-3 w-3" />
              {{ root.path }}
              <span v-if="root.error" class="text-danger">· {{ root.error }}</span>
            </span>
          </div>

          <div class="mt-3 space-y-1.5">
            <div
              v-for="model in entry.filtered"
              :key="model.repo + model.root"
              class="flex flex-wrap items-center gap-2 rounded-md border border-edge/70 bg-void/40 px-3 py-2"
            >
              <div class="min-w-0 flex-1">
                <p class="truncate font-mono text-[11px] text-ink">{{ model.repo }}</p>
                <p class="font-mono text-[9px] uppercase tracking-[0.12em] text-faint">
                  {{ model.root }} · {{ formatBytes(model.size_bytes) }}
                </p>
              </div>
              <span class="chip" :class="model.cached ? 'chip-live' : model.partial ? 'chip-warn' : ''">
                <CheckCircle2 v-if="model.cached" class="h-3 w-3" />
                <AlertTriangle v-else-if="model.partial" class="h-3 w-3" />
                {{ model.cached ? 'cached' : model.partial ? 'orphaned' : 'missing' }}
              </span>
              <span v-if="model.cached && model.partial" class="chip chip-warn">partial blobs</span>
              <span v-if="inUseRepos.has(model.repo)" class="chip chip-warn">in use</span>
              <button
                v-if="!model.cached"
                class="rounded-md border border-edge p-1 text-faint transition hover:border-cyan/40 hover:text-cyan"
                title="Download to this node"
                @click="openDownload(model.repo, entry.node_id)"
              >
                <Download class="h-3.5 w-3.5" />
              </button>
              <button
                v-else
                class="rounded-md border border-edge p-1 text-faint transition hover:border-danger/40 hover:text-danger disabled:opacity-30"
                :disabled="inUseRepos.has(model.repo)"
                title="Remove from this node"
                @click="askRemove(entry, model)"
              >
                <Trash2 class="h-3.5 w-3.5" />
              </button>
            </div>
            <p
              v-if="!entry.filtered.length"
              class="rounded-md border border-dashed border-edge px-3 py-3 font-mono text-[10px] text-faint"
            >
              {{ search ? 'No repos match this filter.' : 'Cache is empty.' }}
            </p>
          </div>
        </div>
      </section>

      <div v-if="!catalog.length" class="panel col-span-full grid place-items-center p-12 text-center">
        <Database class="h-6 w-6 text-faint" />
        <p class="mt-2 text-sm text-dim">No agents registered — add nodes to scan caches.</p>
        <router-link to="/nodes" class="btn btn-primary mt-3">Register a node</router-link>
      </div>
    </div>

    <!-- Download dialog -->
    <div
      v-if="downloadOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="downloadOpen = false"
    >
      <div class="panel w-full max-w-2xl p-6">
        <div class="flex items-center gap-2">
          <Download class="h-4 w-4 text-cyan" />
          <h3 class="text-base font-semibold">Download Model</h3>
          <span v-if="activeCount" class="chip chip-warn ml-2">{{ activeCount }} active</span>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="downloadOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <div class="divider my-4" />
        <div class="grid gap-3 sm:grid-cols-2">
          <div class="sm:col-span-2">
            <label class="hud-label">Repo id</label>
            <input v-model="downloadForm.repo" class="input mt-1" placeholder="org/model" />
          </div>
          <div>
            <label class="hud-label">Revision</label>
            <input v-model="downloadForm.revision" class="input mt-1" placeholder="main / commit" />
          </div>
          <div>
            <label class="hud-label">Cache root (optional)</label>
            <input v-model="downloadForm.root" class="input mt-1" placeholder="default root" />
          </div>
          <div class="sm:col-span-2">
            <label class="hud-label">Allow patterns (comma-separated)</label>
            <input v-model="downloadForm.patterns" class="input mt-1" placeholder="*.safetensors, config.json" />
          </div>
        </div>
        <p class="hud-label mt-4">Target nodes</p>
        <div class="mt-2 flex flex-wrap gap-2">
          <button
            v-for="node in registryNodes"
            :key="node.id"
            class="btn"
            :class="downloadForm.nodes.includes(node.id) ? 'btn-primary' : ''"
            @click="toggleNode(node.id)"
          >
            {{ node.display_name }}
          </button>
        </div>
        <p v-if="downloadError" class="mt-3 font-mono text-[11px] text-danger">{{ downloadError }}</p>
        <div v-if="downloadNodeErrors.length" class="mt-3 space-y-1">
          <p
            v-for="item in downloadNodeErrors"
            :key="item.node_id"
            class="font-mono text-[10px] text-danger"
          >
            {{ nodeName(item.node_id) }}: {{ item.error }}
          </p>
        </div>

        <div v-if="activeJobs.length" class="mt-4 space-y-2">
          <div
            v-for="job in activeJobs"
            :key="`${job.node_id}:${job.id}`"
            class="rounded-md border border-edge bg-void/50 px-3 py-2"
          >
            <div class="flex items-center gap-2">
              <span class="font-mono text-[10px] text-faint">{{ nodeName(job.node_id) }}</span>
              <span class="chip" :class="statusTone(job.state)">{{ statusLabel(job.state) }}</span>
              <span class="truncate font-mono text-[10px] text-dim">{{ job.phase || '' }}</span>
              <span class="ml-auto font-mono text-[9px] text-faint">
                {{ formatBytes(job.bytes_done) }} / {{ formatBytes(job.bytes_total) }}
              </span>
              <button
                v-if="['pending', 'running'].includes(job.state)"
                class="rounded-md border border-edge p-1 text-faint hover:text-danger"
                @click="cancelJob(job)"
              >
                <X class="h-3 w-3" />
              </button>
            </div>
            <div class="mt-1.5 h-1 overflow-hidden rounded-full bg-edge">
              <div
                class="h-full rounded-full bg-gradient-to-r from-cyan to-violet transition-all"
                :style="{ width: `${jobPercent(job)}%` }"
              />
            </div>
            <p class="mt-1 font-mono text-[9px] text-faint">
              {{ job.files_done || 0 }} files · {{ formatPercent(jobPercent(job), 0) }}
              <span v-if="job.error" class="text-danger">· {{ job.error }}</span>
            </p>
          </div>
        </div>

        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="downloadOpen = false">Close</button>
          <button
            class="btn btn-primary"
            :disabled="downloadBusy || !downloadForm.repo || !downloadForm.nodes.length"
            @click="submitDownload"
          >
            <Download class="h-3.5 w-3.5" :class="{ 'animate-bounce': downloadBusy }" />
            Start download
          </button>
        </div>
      </div>
    </div>

    <!-- Remove confirm -->
    <div
      v-if="removeTarget"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="removeTarget = null"
    >
      <div class="panel w-full max-w-md p-6">
        <div class="flex items-center gap-2">
          <Trash2 class="h-4 w-4 text-danger" />
          <h3 class="text-base font-semibold">Remove Model</h3>
        </div>
        <p class="mt-3 text-sm text-dim">
          Delete <span class="font-mono text-ink">{{ removeTarget.model.repo }}</span> from
          <span class="font-mono text-ink">{{ nodeName(removeTarget.nodeId) }}</span>?
        </p>
        <p class="mt-1 font-mono text-[10px] text-faint">
          {{ formatBytes(removeTarget.model.size_bytes) }} · {{ removeTarget.model.root }}
        </p>
        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="removeTarget = null">Cancel</button>
          <button class="btn btn-danger" @click="confirmRemove">Delete files</button>
        </div>
      </div>
    </div>

    <!-- Orphan cleanup confirm -->
    <div
      v-if="cleanupTarget"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="cleanupTarget = null"
    >
      <div class="panel w-full max-w-lg p-6">
        <div class="flex items-center gap-2">
          <AlertTriangle class="h-4 w-4 text-warning" />
          <h3 class="text-base font-semibold">Purge Partial Downloads</h3>
        </div>
        <p class="mt-2 text-sm text-dim">
          {{ cleanupTarget.models.length }} incomplete cache entries on
          {{ nodeName(cleanupTarget.nodeId) }} will be deleted.
        </p>
        <div class="mt-3 max-h-40 overflow-y-auto rounded-md border border-edge bg-void/50 p-2">
          <p
            v-for="model in cleanupTarget.models"
            :key="model.repo + model.root"
            class="font-mono text-[10px] text-dim"
          >
            {{ model.repo }} · {{ formatBytes(model.size_bytes) }}
          </p>
        </div>
        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="cleanupTarget = null">Cancel</button>
          <button class="btn btn-danger" :disabled="cleanupBusy" @click="confirmCleanup">
            Purge {{ cleanupTarget.models.length }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
