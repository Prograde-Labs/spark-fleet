<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  Boxes,
  FileText,
  Plus,
  RefreshCw,
  Search,
  Terminal,
  X,
} from 'lucide-vue-next'
import { api } from '../api'
import { statusLabel } from '../format'
import DeploymentCard from '../components/DeploymentCard.vue'

const items = ref([])
const nodes = ref({})
const recipes = ref([])
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const filter = ref('all')
const search = ref('')
const busy = ref({})
const jobs = ref({})
const logModal = ref(null)
const createOpen = ref(false)
const createBusy = ref(false)
const createError = ref(null)
const newDeployment = ref({ recipe: '', name: '', primary: false })

let refreshTimer = null
let unmounted = false

const filtered = computed(() => {
  const term = search.value.trim().toLowerCase()
  return items.value.filter((item) => {
    const status = item.deployment.status
    if (filter.value === 'loaded' && status !== 'loaded') return false
    if (filter.value === 'unloaded' && !['unloaded', 'pending'].includes(status)) return false
    if (filter.value === 'failed' && status !== 'failed') return false
    if (!term) return true
    return (
      item.deployment.name.toLowerCase().includes(term) ||
      item.deployment.recipe_name.toLowerCase().includes(term)
    )
  })
})

const counts = computed(() => ({
  all: items.value.length,
  loaded: items.value.filter((i) => i.deployment.status === 'loaded').length,
  unloaded: items.value.filter((i) =>
    ['unloaded', 'pending'].includes(i.deployment.status),
  ).length,
  failed: items.value.filter((i) => i.deployment.status === 'failed').length,
}))

const filters = [
  { key: 'all', label: 'All' },
  { key: 'loaded', label: 'Loaded' },
  { key: 'unloaded', label: 'Idle' },
  { key: 'failed', label: 'Failed' },
]

async function load() {
  error.value = null
  try {
    const [deploymentResponse, nodeResponse, recipeResponse] = await Promise.all([
      api.deployments.list(),
      api.nodes.list(),
      api.recipes.list(),
    ])
    items.value = deploymentResponse.deployments || []
    nodes.value = Object.fromEntries(
      (nodeResponse.nodes || []).map((node) => [node.id, node]),
    )
    recipes.value = recipeResponse.recipes || []
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function trackJob(deploymentId, jobId) {
  jobs.value = {
    ...jobs.value,
    [deploymentId]: { id: jobId, status: 'pending', phase: 'queued' },
  }
  const started = Date.now()
  while (Date.now() - started < 60 * 60 * 1000) {
    await sleep(1500)
    if (unmounted) return
    try {
      const response = await api.jobs.get(jobId)
      const job = response.job || {}
      jobs.value = { ...jobs.value, [deploymentId]: job }
      if (['succeeded', 'failed', 'cancelled'].includes(job.status)) {
        const failed = job.status === 'failed'
        await load()
        if (failed) {
          error.value = job.error || `${job.kind} failed`
        } else {
          notice.value = `${job.kind} ${statusLabel(job.status)}`
        }
        setTimeout(() => {
          if (unmounted) return
          const next = { ...jobs.value }
          delete next[deploymentId]
          jobs.value = next
        }, 6000)
        return
      }
    } catch (exc) {
      error.value = exc.message || String(exc)
      return
    }
  }
}

async function runAction(deployment, action, operation) {
  busy.value = { ...busy.value, [deployment.id]: action }
  error.value = null
  notice.value = null
  try {
    const response = await operation()
    if (response?.job_id) await trackJob(deployment.id, response.job_id)
    else await load()
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    const next = { ...busy.value }
    delete next[deployment.id]
    busy.value = next
  }
}

const loadDeployment = (deployment) =>
  runAction(deployment, 'load', () => api.deployments.load(deployment.id))
const unloadDeployment = (deployment) =>
  runAction(deployment, 'unload', () => api.deployments.unload(deployment.id))
const redeployDeployment = (deployment) =>
  runAction(deployment, 'redeploy', () => api.deployments.redeploy(deployment.id))
const makePrimary = (deployment) =>
  runAction(deployment, 'primary', () => api.deployments.setPrimary(deployment.id))
const deleteDeployment = (deployment) =>
  runAction(deployment, 'delete', () => api.deployments.remove(deployment.id))

async function openLogs(role) {
  logModal.value = { role, logs: '', loading: true, error: null }
  try {
    const response = await api.deployments.roleLogs(role.deployment_id, role.role_name)
    logModal.value = { ...logModal.value, logs: response.logs || '', loading: false }
  } catch (exc) {
    logModal.value = { ...logModal.value, loading: false, error: exc.message }
  }
}

function roleWithDeployment(item, role) {
  return { ...role, deployment_id: item.deployment.id, deployment_name: item.deployment.name }
}

function openCreate() {
  createError.value = null
  newDeployment.value = {
    recipe: recipes.value[0]?.name || '',
    name: recipes.value[0]?.name || '',
    primary: false,
  }
  createOpen.value = true
}

function onRecipeChange() {
  const recipe = recipes.value.find((item) => item.name === newDeployment.value.recipe)
  if (recipe) newDeployment.value.name = recipe.name
}

async function createDeployment() {
  createBusy.value = true
  createError.value = null
  try {
    const created = await api.deployments.create({
      recipe: newDeployment.value.recipe,
      name: newDeployment.value.name,
    })
    createOpen.value = false
    await load()
    if (newDeployment.value.primary) {
      await runAction(
        created.deployment,
        'load',
        () => api.deployments.load(created.deployment.id, true),
      )
    }
  } catch (exc) {
    createError.value = exc.message || String(exc)
  } finally {
    createBusy.value = false
  }
}

onMounted(async () => {
  await load()
  refreshTimer = setInterval(load, 20000)
})

onBeforeUnmount(() => {
  unmounted = true
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Deployment Control</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Fleet Workloads</h3>
      </div>
      <div class="ml-auto flex flex-wrap items-center gap-2">
        <div class="relative">
          <Search class="pointer-events-none absolute left-2.5 top-2.5 h-3.5 w-3.5 text-faint" />
          <input
            v-model="search"
            class="input w-56 pl-8"
            placeholder="Search deployments…"
          />
        </div>
        <button class="btn" :disabled="loading" @click="load">
          <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': loading }" />
          Sync
        </button>
        <button class="btn btn-primary" @click="openCreate">
          <Plus class="h-3.5 w-3.5" />
          New Deployment
        </button>
      </div>
    </header>

    <div class="flex flex-wrap items-center gap-2">
      <button
        v-for="entry in filters"
        :key="entry.key"
        class="btn"
        :class="filter === entry.key ? 'btn-primary' : ''"
        @click="filter = entry.key"
      >
        {{ entry.label }}
        <span class="text-[9px] text-faint">{{ counts[entry.key] }}</span>
      </button>
    </div>

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

    <div v-if="loading && !items.length" class="panel grid place-items-center p-12">
      <RefreshCw class="h-5 w-5 animate-spin text-cyan" />
      <p class="mt-2 font-mono text-[11px] text-faint">Loading fleet state…</p>
    </div>

    <div v-else class="grid gap-4 xl:grid-cols-2">
      <DeploymentCard
        v-for="item in filtered"
        :key="item.deployment.id"
        :item="item"
        :nodes="nodes"
        :busy="busy[item.deployment.id]"
        :job="jobs[item.deployment.id]"
        @load="loadDeployment"
        @unload="unloadDeployment"
        @redeploy="redeployDeployment"
        @primary="makePrimary"
        @delete="deleteDeployment"
        @logs="(role) => openLogs(roleWithDeployment(item, role))"
      />
      <div v-if="!filtered.length" class="panel col-span-full grid place-items-center p-12 text-center">
        <Boxes class="h-6 w-6 text-faint" />
        <p class="mt-2 text-sm text-dim">
          {{ items.length ? 'No deployments match this filter.' : 'No deployments registered.' }}
        </p>
        <button v-if="!items.length" class="btn btn-primary mt-3" @click="openCreate">
          Create from recipe
        </button>
      </div>
    </div>

    <!-- Create dialog -->
    <div
      v-if="createOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="createOpen = false"
    >
      <div class="panel w-full max-w-lg p-6">
        <div class="flex items-center gap-2">
          <Boxes class="h-4 w-4 text-cyan" />
          <h3 class="text-base font-semibold">New Deployment</h3>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="createOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <div class="divider my-4" />
        <label class="hud-label">Recipe</label>
        <select v-model="newDeployment.recipe" class="input mt-1" @change="onRecipeChange">
          <option v-for="recipe in recipes" :key="recipe.name" :value="recipe.name">
            {{ recipe.name }} · {{ recipe.mode }} · {{ recipe.servedName || 'service' }}
          </option>
        </select>
        <label class="hud-label mt-4 block">Deployment name</label>
        <input v-model="newDeployment.name" class="input mt-1" placeholder="demo-glm" />
        <label class="mt-4 flex items-center gap-2 font-mono text-[11px] text-dim">
          <input v-model="newDeployment.primary" type="checkbox" class="accent-cyan" />
          Load immediately as spark-active
        </label>
        <p v-if="createError" class="mt-3 font-mono text-[11px] text-danger">{{ createError }}</p>
        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="createOpen = false">Cancel</button>
          <button
            class="btn btn-primary"
            :disabled="createBusy || !newDeployment.recipe || !newDeployment.name"
            @click="createDeployment"
          >
            <Plus class="h-3.5 w-3.5" />
            Create
          </button>
        </div>
      </div>
    </div>

    <!-- Logs dialog -->
    <div
      v-if="logModal"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="logModal = null"
    >
      <div class="panel w-full max-w-3xl overflow-hidden">
        <div class="flex items-center gap-2 border-b border-edge px-5 py-3">
          <Terminal class="h-4 w-4 text-cyan" />
          <div>
            <p class="text-sm font-semibold">
              {{ logModal.role.deployment_name }} / {{ logModal.role.role_name }}
            </p>
            <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
              {{ logModal.role.node_id }} · {{ logModal.role.container_id }}
            </p>
          </div>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="logModal = null">
            <X class="h-4 w-4" />
          </button>
        </div>
        <div class="max-h-[60vh] overflow-y-auto bg-void/70 p-4">
          <p v-if="logModal.loading" class="font-mono text-[11px] text-faint">Reading logs…</p>
          <p v-else-if="logModal.error" class="font-mono text-[11px] text-danger">
            {{ logModal.error }}
          </p>
          <pre v-else class="whitespace-pre-wrap font-mono text-[11px] leading-relaxed text-dim">{{ logModal.logs || 'No output.' }}</pre>
        </div>
        <div class="flex items-center gap-2 border-t border-edge px-5 py-3">
          <FileText class="h-3.5 w-3.5 text-faint" />
          <span class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
            Last 200 lines
          </span>
          <button class="btn ml-auto" @click="logModal = null">Close</button>
        </div>
      </div>
    </div>
  </div>
</template>
