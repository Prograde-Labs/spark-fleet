<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import {
  AlertTriangle,
  ArrowRight,
  Check,
  DownloadCloud,
  FileCode2,
  Plus,
  RefreshCw,
  Server,
  Trash2,
} from 'lucide-vue-next'
import { api } from '../api'
import YamlDiff from '../components/YamlDiff.vue'

const nodes = ref([])
const sources = ref([])
const plan = ref(null)
const skipped = ref([])
const scanning = ref(false)
const applying = ref(false)
const error = ref(null)
const notice = ref(null)
const confirmed = ref(false)
const expanded = ref({})
const created = ref([])

const proposals = computed(() => plan.value?.proposals || [])
const validSources = computed(() =>
  sources.value.filter((source) => source.nodeId && source.url.trim()),
)

async function loadNodes() {
  try {
    const response = await api.nodes.list()
    nodes.value = response.nodes || []
    if (!sources.value.length) {
      sources.value = nodes.value.map((node) => ({
        nodeId: node.id,
        url: `http://${node.lan_ip}:8091`,
      }))
    }
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

function addSource() {
  sources.value.push({ nodeId: nodes.value[0]?.id || '', url: '' })
}

function removeSource(index) {
  sources.value.splice(index, 1)
}

async function scan() {
  scanning.value = true
  error.value = null
  notice.value = null
  confirmed.value = false
  try {
    const response = await api.import.plan(validSources.value)
    plan.value = response
    skipped.value = response.skipped || []
    created.value = []
  } catch (exc) {
    error.value = exc.message || String(exc)
    plan.value = null
  } finally {
    scanning.value = false
  }
}

async function apply() {
  if (!confirmed.value) return
  applying.value = true
  error.value = null
  try {
    const response = await api.import.apply(validSources.value)
    created.value = response.created || []
    notice.value = `${created.value.length} deployment(s) imported as pending recipes`
    confirmed.value = false
    plan.value = null
    skipped.value = []
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    applying.value = false
  }
}

function toggleYaml(index) {
  expanded.value = { ...expanded.value, [index]: !expanded.value[index] }
}

watch(
  sources,
  () => {
    confirmed.value = false
  },
  { deep: true },
)

onMounted(loadNodes)
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Migration</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Import Old Controllers</h3>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <button class="btn" :disabled="scanning" @click="scan">
          <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': scanning }" />
          Scan sources
        </button>
      </div>
    </header>

    <div class="rounded-lg border border-warning/30 bg-warning/[0.06] px-4 py-3">
      <p class="flex items-center gap-2 font-mono text-[11px] text-warning">
        <AlertTriangle class="h-4 w-4" />
        Import reads the old legacy-controller controllers; nothing is stopped or cut over here.
      </p>
      <p class="mt-1 font-mono text-[10px] text-faint">
        Proposed recipes and diffs are shown before apply. Apply writes pending
        deployments and recipe files only.
      </p>
    </div>

    <section class="panel p-5">
      <header class="flex items-center gap-2">
        <DownloadCloud class="h-4 w-4 text-cyan" />
        <div>
          <p class="hud-label text-cyan/70">Sources</p>
          <h4 class="text-sm font-semibold">Old Controller Endpoints</h4>
        </div>
        <button class="btn ml-auto" @click="addSource">
          <Plus class="h-3.5 w-3.5" /> Add source
        </button>
      </header>
      <div class="divider my-4" />
      <div class="space-y-2">
        <div
          v-for="(source, index) in sources"
          :key="index"
          class="grid gap-2 rounded-md border border-edge bg-void/40 p-3 sm:grid-cols-[200px_1fr_auto]"
        >
          <div>
            <label class="hud-label">Fleet node</label>
            <select v-model="source.nodeId" class="input mt-1">
              <option v-for="node in nodes" :key="node.id" :value="node.id">
                {{ node.display_name }} ({{ node.id }})
              </option>
            </select>
          </div>
          <div>
            <label class="hud-label">Old controller URL</label>
            <input v-model="source.url" class="input mt-1" placeholder="http://192.0.2.239:8091" />
          </div>
          <button
            class="mt-5 h-fit rounded-md border border-edge p-2 text-faint transition hover:border-danger/40 hover:text-danger"
            title="Remove source"
            @click="removeSource(index)"
          >
            <Trash2 class="h-3.5 w-3.5" />
          </button>
        </div>
        <p v-if="!sources.length" class="rounded-md border border-dashed border-edge px-3 py-4 text-center font-mono text-[10px] text-faint">
          Add an old controller endpoint to scan.
        </p>
      </div>
      <p v-if="!nodes.length" class="mt-3 font-mono text-[10px] text-warning">
        Register nodes first so imported roles can map to fleet node ids.
      </p>
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
      <router-link to="/deployments" class="btn ml-auto">
        <ArrowRight class="h-3.5 w-3.5" /> Review deployments
      </router-link>
    </div>

    <section v-if="created.length" class="panel p-5">
      <header class="flex items-center gap-2">
        <Check class="h-4 w-4 text-signal" />
        <div>
          <p class="hud-label text-signal/70">Imported</p>
          <h4 class="text-sm font-semibold">Pending Deployments</h4>
        </div>
      </header>
      <div class="mt-3 flex flex-wrap gap-2">
        <span v-for="id in created" :key="id" class="chip chip-live">{{ id }}</span>
      </div>
    </section>

    <section v-if="plan" class="space-y-4">
      <div class="flex flex-wrap items-center gap-2">
        <div>
          <p class="hud-label text-violet/70">Proposed Recipes</p>
          <h4 class="text-sm font-semibold">
            {{ proposals.length }} grouping(s) · {{ skipped.length }} skipped
          </h4>
        </div>
        <label class="ml-auto flex items-center gap-2 font-mono text-[11px] text-dim">
          <input v-model="confirmed" type="checkbox" class="accent-cyan" />
          I reviewed the diffs and want to apply
        </label>
        <button
          class="btn btn-primary"
          :disabled="!confirmed || applying || !proposals.length"
          @click="apply"
        >
          <Check class="h-3.5 w-3.5" :class="{ 'animate-pulse': applying }" />
          Apply import
        </button>
      </div>

      <div class="grid gap-4 xl:grid-cols-2">
        <article
          v-for="(proposal, index) in proposals"
          :key="proposal.name"
          class="panel overflow-hidden"
        >
          <header class="flex flex-wrap items-center gap-2 border-b border-edge px-4 py-3">
            <FileCode2 class="h-4 w-4 text-cyan" />
            <div class="min-w-0">
              <p class="truncate font-mono text-[12px] text-ink">{{ proposal.name }}</p>
              <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
                {{ proposal.recipe.spec.mode }} · {{ proposal.recipe.spec.roles.length }} roles ·
                {{ proposal.recipe.spec.model?.servedName || 'service' }}
              </p>
            </div>
            <button class="btn ml-auto" @click="toggleYaml(index)">
              {{ expanded[index] ? 'Hide YAML' : 'Recipe YAML' }}
            </button>
          </header>
          <div class="p-4">
            <YamlDiff :diff="proposal.diff" />
            <pre
              v-if="expanded[index]"
              class="mt-3 max-h-72 overflow-auto rounded-md border border-edge bg-void/60 p-3 font-mono text-[10px] leading-relaxed text-dim"
            >{{ proposal.yaml }}</pre>
          </div>
        </article>
        <div v-if="!proposals.length" class="panel col-span-full grid place-items-center p-10 text-center">
          <Server class="h-5 w-5 text-faint" />
          <p class="mt-2 text-sm text-dim">No importable groupings were proposed.</p>
        </div>
      </div>

      <div v-if="skipped.length" class="panel p-5">
        <header class="flex items-center gap-2">
          <AlertTriangle class="h-4 w-4 text-warning" />
          <div>
            <p class="hud-label text-warning/70">Skipped</p>
            <h4 class="text-sm font-semibold">Workloads Not Imported</h4>
          </div>
        </header>
        <div class="mt-3 space-y-1">
          <div
            v-for="item in skipped"
            :key="item.name"
            class="flex flex-wrap items-center gap-2 rounded-md border border-edge/70 bg-void/40 px-3 py-1.5"
          >
            <span class="font-mono text-[11px] text-ink">{{ item.name }}</span>
            <span class="font-mono text-[10px] text-faint">{{ item.reason }}</span>
          </div>
        </div>
      </div>
    </section>

    <div v-else-if="scanning" class="panel grid place-items-center p-12">
      <RefreshCw class="h-5 w-5 animate-spin text-cyan" />
      <p class="mt-2 font-mono text-[11px] text-faint">Scanning old controllers…</p>
    </div>
  </div>
</template>
