<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import {
  Activity,
  Ban,
  CheckCircle2,
  Clock,
  RefreshCw,
  Search,
  Terminal,
  XCircle,
} from 'lucide-vue-next'
import { api } from '../api'
import { formatRelative, formatTime, shortId, statusLabel, statusTone } from '../format'

const jobs = ref([])
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const filter = ref('all')
const search = ref('')
const selected = ref(null)
const detail = ref(null)
const detailBusy = ref(false)
const cancelling = ref(null)

let refreshTimer = null

const filters = [
  { key: 'all', label: 'All' },
  { key: 'active', label: 'Active' },
  { key: 'failed', label: 'Failed' },
  { key: 'succeeded', label: 'Succeeded' },
  { key: 'cancelled', label: 'Cancelled' },
]

const filtered = computed(() => {
  const term = search.value.trim().toLowerCase()
  return jobs.value.filter((job) => {
    const status = job.status
    if (filter.value === 'active' && !['pending', 'running'].includes(status)) return false
    if (filter.value === 'failed' && status !== 'failed') return false
    if (filter.value === 'succeeded' && status !== 'succeeded') return false
    if (filter.value === 'cancelled' && status !== 'cancelled') return false
    if (!term) return true
    return (
      job.id.toLowerCase().includes(term) ||
      job.kind.toLowerCase().includes(term) ||
      (job.deployment_id || '').toLowerCase().includes(term)
    )
  })
})

const counts = computed(() => ({
  all: jobs.value.length,
  active: jobs.value.filter((job) => ['pending', 'running'].includes(job.status)).length,
  failed: jobs.value.filter((job) => job.status === 'failed').length,
  succeeded: jobs.value.filter((job) => job.status === 'succeeded').length,
  cancelled: jobs.value.filter((job) => job.status === 'cancelled').length,
}))

async function load() {
  error.value = null
  try {
    const response = await api.jobs.list(200)
    jobs.value = response.jobs || []
    if (selected.value) {
      const current = jobs.value.find((job) => job.id === selected.value.id)
      if (current) {
        selected.value = current
        await loadDetail(current.id, false)
      }
    }
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

async function loadDetail(jobId, showBusy = true) {
  if (showBusy) detailBusy.value = true
  try {
    const response = await api.jobs.get(jobId)
    detail.value = response
    selected.value = response.job
  } catch (exc) {
    error.value = exc.message || String(exc)
    detail.value = null
  } finally {
    detailBusy.value = false
  }
}

async function selectJob(job) {
  selected.value = job
  await loadDetail(job.id)
}

async function cancelJob(job) {
  cancelling.value = job.id
  try {
    const response = await api.jobs.cancel(job.id)
    notice.value = response.cancelled ? `Cancellation requested for ${shortId(job.id)}` : 'Job is not cancellable'
    await load()
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    cancelling.value = null
  }
}

function pretty(value) {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'string') return value
  try {
    return JSON.stringify(value, null, 2)
  } catch {
    return String(value)
  }
}

function eventTone(event) {
  if (event.level === 'error') return 'text-danger'
  if (event.level === 'warning') return 'text-warning'
  return 'text-dim'
}

onMounted(async () => {
  await load()
  refreshTimer = setInterval(load, 8000)
})

onBeforeUnmount(() => {
  if (refreshTimer) clearInterval(refreshTimer)
})
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Job Telemetry</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Activity Log</h3>
      </div>
      <div class="ml-auto flex flex-wrap items-center gap-2">
        <div class="relative">
          <Search class="pointer-events-none absolute left-2.5 top-2.5 h-3.5 w-3.5 text-faint" />
          <input v-model="search" class="input w-56 pl-8" placeholder="Search jobs…" />
        </div>
        <button class="btn" :disabled="loading" @click="load">
          <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': loading }" />
          Refresh
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
      class="flex items-center gap-3 rounded-lg border border-cyan/30 bg-cyan/[0.06] px-4 py-3"
    >
      <p class="font-mono text-xs text-cyan-soft">{{ notice }}</p>
      <button class="btn ml-auto" @click="notice = null">Dismiss</button>
    </div>

    <div class="grid gap-5 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
      <section class="panel overflow-hidden">
        <header class="flex items-center gap-2 border-b border-edge px-4 py-3">
          <Activity class="h-4 w-4 text-cyan" />
          <span class="hud-label text-cyan/70">Job Queue</span>
          <span class="chip ml-auto">{{ filtered.length }} shown</span>
        </header>
        <div class="max-h-[68vh] overflow-y-auto">
          <button
            v-for="job in filtered"
            :key="job.id"
            class="flex w-full items-center gap-3 border-b border-edge/60 px-4 py-3 text-left transition last:border-0 hover:bg-raised/40"
            :class="selected?.id === job.id ? 'bg-cyan/[0.05]' : ''"
            @click="selectJob(job)"
          >
            <span
              class="grid h-8 w-8 place-items-center rounded-md border"
              :class="
                job.status === 'failed'
                  ? 'border-danger/30 text-danger'
                  : job.status === 'succeeded'
                    ? 'border-signal/30 text-signal'
                    : 'border-edge text-cyan'
              "
            >
              <XCircle v-if="job.status === 'failed'" class="h-4 w-4" />
              <CheckCircle2 v-else-if="job.status === 'succeeded'" class="h-4 w-4" />
              <Clock v-else class="h-4 w-4" />
            </span>
            <div class="min-w-0 flex-1">
              <div class="flex items-center gap-2">
                <p class="truncate font-mono text-[11px] text-ink">{{ job.kind }}</p>
                <span class="chip" :class="statusTone(job.status)">
                  {{ statusLabel(job.status) }}
                </span>
              </div>
              <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
                {{ shortId(job.id) }} ·
                {{ job.deployment_id ? shortId(job.deployment_id) : 'no deployment' }} ·
                {{ formatRelative(job.created_at) }}
              </p>
            </div>
            <button
              v-if="['pending', 'running'].includes(job.status)"
              class="rounded-md border border-edge p-1 text-faint transition hover:border-danger/40 hover:text-danger"
              :disabled="cancelling === job.id"
              title="Cancel job"
              @click.stop="cancelJob(job)"
            >
              <Ban class="h-3.5 w-3.5" />
            </button>
          </button>
          <p v-if="!filtered.length" class="px-4 py-10 text-center font-mono text-[11px] text-faint">
            No jobs match this filter.
          </p>
        </div>
      </section>

      <section class="panel overflow-hidden">
        <header class="flex items-center gap-2 border-b border-edge px-4 py-3">
          <Terminal class="h-4 w-4 text-violet" />
          <span class="hud-label text-violet/70">Job Detail</span>
          <button v-if="selected" class="btn ml-auto" :disabled="detailBusy" @click="loadDetail(selected.id)">
            <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': detailBusy }" />
          </button>
        </header>
        <div v-if="!selected" class="grid place-items-center p-10 text-center">
          <Terminal class="h-5 w-5 text-faint" />
          <p class="mt-2 text-sm text-dim">Select a job to inspect phases and payload.</p>
        </div>
        <div v-else class="max-h-[68vh] overflow-y-auto p-4">
          <div class="grid gap-2 sm:grid-cols-2">
            <div class="panel-raised p-3">
              <p class="hud-label">Kind</p>
              <p class="hud-value mt-1 text-[12px]">{{ selected.kind }}</p>
            </div>
            <div class="panel-raised p-3">
              <p class="hud-label">Status</p>
              <p class="mt-1">
                <span class="chip" :class="statusTone(selected.status)">
                  {{ statusLabel(selected.status) }}
                </span>
              </p>
            </div>
            <div class="panel-raised p-3">
              <p class="hud-label">Created</p>
              <p class="hud-value mt-1 text-[12px]">{{ formatTime(selected.created_at) }}</p>
            </div>
            <div class="panel-raised p-3">
              <p class="hud-label">Finished</p>
              <p class="hud-value mt-1 text-[12px]">
                {{ selected.finished_at ? formatTime(selected.finished_at) : '—' }}
              </p>
            </div>
          </div>

          <p v-if="selected.error" class="mt-3 rounded-md border border-danger/30 bg-danger/10 px-3 py-2 font-mono text-[11px] text-danger">
            {{ selected.error }}
          </p>

          <div class="mt-4">
            <p class="hud-label">Phases</p>
            <div class="mt-2 space-y-1">
              <div
                v-for="event in detail?.events || []"
                :key="event.id"
                class="flex items-start gap-2 rounded-md border border-edge/60 bg-void/40 px-2.5 py-1.5"
              >
                <span class="w-16 shrink-0 font-mono text-[9px] tabular-nums text-faint">
                  {{ formatTime(event.ts) }}
                </span>
                <span class="w-20 shrink-0 font-mono text-[9px] uppercase tracking-[0.12em] text-faint">
                  {{ event.phase || event.level }}
                </span>
                <span class="font-mono text-[10px]" :class="eventTone(event)">
                  {{ event.message }}
                </span>
              </div>
              <p v-if="!(detail?.events || []).length" class="font-mono text-[10px] text-faint">
                No phase events recorded.
              </p>
            </div>
          </div>

          <div class="mt-4 grid gap-3 lg:grid-cols-2">
            <div>
              <p class="hud-label">Payload</p>
              <pre class="mt-1 max-h-48 overflow-auto rounded-md border border-edge bg-void/60 p-2 font-mono text-[10px] text-dim">{{ pretty(selected.payload) }}</pre>
            </div>
            <div>
              <p class="hud-label">Result</p>
              <pre class="mt-1 max-h-48 overflow-auto rounded-md border border-edge bg-void/60 p-2 font-mono text-[10px] text-dim">{{ pretty(selected.result) }}</pre>
            </div>
          </div>
        </div>
      </section>
    </div>
  </div>
</template>
