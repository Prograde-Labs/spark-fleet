<script setup>
import { computed, onMounted, ref, watch } from 'vue'
import yaml from 'js-yaml'
import {
  ArrowRightLeft,
  Check,
  Copy,
  FileCode2,
  Plus,
  RefreshCw,
  Save,
  Trash2,
  X,
} from 'lucide-vue-next'
import { api } from '../api'
import RecipeForm from '../components/RecipeForm.vue'
import YamlDiff from '../components/YamlDiff.vue'

const recipes = ref([])
const nodes = ref([])
const selected = ref(null)
const draft = ref(null)
const yamlText = ref('')
const tab = ref('structured')
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const validation = ref(null)
const savedYaml = ref('')
const saving = ref(false)
const deleteArmed = ref(null)
const swapOpen = ref(false)
const swapBusy = ref(false)
const swapPreview = ref(null)
const swapError = ref(null)
const swapForm = ref({ repo: '', servedName: '', revision: '' })
const duplicateOpen = ref(false)
const duplicateName = ref('')
const duplicateError = ref(null)

const isNew = computed(() => selected.value === null)
const hasModel = computed(() => Boolean(draft.value?.spec?.model))
const dirty = computed(() => {
  if (!draft.value) return false
  const current =
    tab.value === 'yaml'
      ? yamlText.value
      : yaml.dump(draft.value, { noRefs: true, lineWidth: 100 })
  return current !== savedYaml.value
})

const recipeList = computed(() =>
  [...recipes.value].sort((a, b) => a.name.localeCompare(b.name)),
)

function blankRecipe() {
  return {
    apiVersion: 'spark-fleet/v1',
    kind: 'Recipe',
    metadata: { name: 'new-recipe', displayName: '', description: '' },
    spec: {
      mode: 'deployment',
      runtime: 'custom',
      topology: { preset: 'single', masterPort: 25000 },
      model: null,
      gateway: null,
      roles: [],
      prepare: [],
    },
  }
}

function normalize(recipe) {
  const copy = structuredClone(recipe)
  copy.metadata = copy.metadata || {}
  copy.spec = copy.spec || {}
  copy.spec.topology = copy.spec.topology || { preset: 'single', masterPort: 25000 }
  if (!Number.isInteger(copy.spec.topology.masterPort) || copy.spec.topology.masterPort <= 0) {
    copy.spec.topology.masterPort = 25000
  }
  copy.spec.roles = copy.spec.roles || []
  copy.spec.prepare = copy.spec.prepare || []
  for (const role of copy.spec.roles) {
    role.service = role.service || {}
    role.readiness = role.readiness || { type: 'running', timeoutSeconds: 3600 }
    if (!role.readiness.type) role.readiness.type = 'running'
    if (!role.readiness.timeoutSeconds) role.readiness.timeoutSeconds = 3600
    if (role.readiness.type === 'http' && !role.readiness.path) {
      role.readiness.path = '/v1/models'
    }
    if (role.startOrder === undefined) role.startOrder = 0
    if (role.inferencePort === undefined) role.inferencePort = null
  }
  return copy
}

async function loadList() {
  loading.value = true
  try {
    const [recipeResponse, nodeResponse] = await Promise.all([
      api.recipes.list(),
      api.nodes.list(),
    ])
    recipes.value = recipeResponse.recipes || []
    nodes.value = nodeResponse.nodes || []
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

async function selectRecipe(name) {
  error.value = null
  notice.value = null
  validation.value = null
  try {
    const response = await api.recipes.get(name)
    yamlText.value = response.yaml
    draft.value = normalize(yaml.load(response.yaml))
    savedYaml.value = yaml.dump(draft.value, { noRefs: true, lineWidth: 100 })
    selected.value = name
    tab.value = 'structured'
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

function newRecipe() {
  draft.value = blankRecipe()
  yamlText.value = yaml.dump(draft.value, { noRefs: true, lineWidth: 100 })
  savedYaml.value = yamlText.value
  selected.value = null
  validation.value = null
  tab.value = 'structured'
  notice.value = null
  deleteArmed.value = null
}

function syncYaml() {
  if (!draft.value) return
  normalizeNumbers()
  yamlText.value = yaml.dump(draft.value, { noRefs: true, lineWidth: 100 })
}

function normalizeNumbers() {
  if (!draft.value) return
  const spec = draft.value.spec
  const master = Number(spec.topology?.masterPort)
  spec.topology.masterPort =
    Number.isInteger(master) && master > 0 ? master : 25000
  for (const role of spec.roles || []) {
    role.startOrder = Number.isInteger(Number(role.startOrder))
      ? Number(role.startOrder)
      : 0
    role.inferencePort =
      role.inferencePort === '' || role.inferencePort === null
        ? null
        : Number(role.inferencePort)
    role.readiness = role.readiness || { type: 'running', timeoutSeconds: 3600 }
    role.readiness.timeoutSeconds =
      Number(role.readiness.timeoutSeconds) > 0
        ? Number(role.readiness.timeoutSeconds)
        : 3600
    if (role.readiness.type === 'http' && !role.readiness.path) {
      role.readiness.path = '/v1/models'
    }
    if (role.readiness.port === '') delete role.readiness.port
  }
}

function applyYaml() {
  try {
    draft.value = normalize(yaml.load(yamlText.value))
    error.value = null
    return true
  } catch (exc) {
    error.value = `YAML parse error: ${exc.message}`
    return false
  }
}

function switchTab(next) {
  if (next === 'yaml' && tab.value === 'structured') syncYaml()
  if (next === 'structured' && tab.value === 'yaml') {
    if (!applyYaml()) return
  }
  tab.value = next
}

function currentYaml() {
  if (tab.value === 'yaml') {
    if (!applyYaml()) return null
  }
  normalizeNumbers()
  syncYaml()
  return yamlText.value
}

async function validateRecipe() {
  const text = currentYaml()
  if (text === null) return
  error.value = null
  try {
    validation.value = await api.recipes.validate({ yaml: text })
    tab.value = 'preview'
    notice.value = 'Recipe validated'
  } catch (exc) {
    error.value = exc.message || String(exc)
    validation.value = null
  }
}

async function saveRecipe() {
  const text = currentYaml()
  if (text === null) return
  const name = draft.value?.metadata?.name
  if (!name) {
    error.value = 'Recipe name is required'
    return
  }
  saving.value = true
  error.value = null
  try {
    const response = await api.recipes.put(name, text)
    yamlText.value = response.yaml
    draft.value = normalize(yaml.load(response.yaml))
    savedYaml.value = yaml.dump(draft.value, { noRefs: true, lineWidth: 100 })
    selected.value = name
    notice.value = `Saved ${name}`
    await loadList()
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    saving.value = false
  }
}

async function deleteRecipe() {
  if (!selected.value) return
  if (deleteArmed.value !== selected.value) {
    deleteArmed.value = selected.value
    return
  }
  deleteArmed.value = null
  try {
    await api.recipes.remove(selected.value)
    notice.value = `Deleted ${selected.value}`
    selected.value = null
    draft.value = null
    yamlText.value = ''
    savedYaml.value = ''
    await loadList()
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

function openDuplicate() {
  if (!draft.value) return
  duplicateName.value = `${draft.value.metadata.name}-copy`
  duplicateError.value = null
  duplicateOpen.value = true
}

async function duplicateRecipe() {
  if (!draft.value) return
  duplicateError.value = null
  const name = duplicateName.value.trim()
  try {
    const copy = JSON.parse(JSON.stringify(draft.value))
    copy.metadata.name = name
    const response = await api.recipes.put(name, yaml.dump(copy, { noRefs: true }))
    duplicateOpen.value = false
    await loadList()
    await selectRecipe(name)
    notice.value = `Duplicated as ${name}`
  } catch (exc) {
    duplicateError.value = exc.message || String(exc)
  }
}

function openSwap() {
  const model = draft.value?.spec?.model
  if (!model) return
  swapForm.value = {
    repo: model.repo || '',
    servedName: model.servedName || '',
    revision: model.revision || '',
  }
  swapPreview.value = null
  swapError.value = null
  swapOpen.value = true
}

async function previewSwap() {
  swapBusy.value = true
  swapError.value = null
  try {
    swapPreview.value = await api.recipes.swapModel(selected.value, {
      repo: swapForm.value.repo,
      servedName: swapForm.value.servedName,
      revision: swapForm.value.revision || undefined,
    })
  } catch (exc) {
    swapError.value = exc.message || String(exc)
  } finally {
    swapBusy.value = false
  }
}

async function applySwap() {
  swapBusy.value = true
  swapError.value = null
  try {
    await api.recipes.swapModel(selected.value, {
      repo: swapForm.value.repo,
      servedName: swapForm.value.servedName,
      revision: swapForm.value.revision || undefined,
      apply: true,
    })
    swapOpen.value = false
    await selectRecipe(selected.value)
    await loadList()
    notice.value = 'Model swap applied — redeploy to activate'
  } catch (exc) {
    swapError.value = exc.message || String(exc)
  } finally {
    swapBusy.value = false
  }
}

function composeYaml(compose) {
  return yaml.dump(compose, { noRefs: true, lineWidth: 100 })
}

onMounted(async () => {
  await loadList()
  if (recipes.value.length) await selectRecipe(recipes.value[0].name)
  else newRecipe()
})
</script>

<template>
  <div class="grid gap-5 lg:grid-cols-[250px_minmax(0,1fr)]">
    <!-- Recipe rail -->
    <aside class="panel h-fit p-3">
      <div class="flex items-center gap-2 px-1 pb-2">
        <FileCode2 class="h-4 w-4 text-cyan" />
        <span class="hud-label text-cyan/70">Recipe Vault</span>
        <button class="btn ml-auto" title="New recipe" @click="newRecipe">
          <Plus class="h-3.5 w-3.5" />
        </button>
      </div>
      <div class="divider mb-2" />
      <button
        v-for="recipe in recipeList"
        :key="recipe.name"
        class="mb-1 flex w-full items-center gap-2 rounded-md border border-transparent px-2 py-2 text-left transition hover:border-edge hover:bg-raised/60"
        :class="selected === recipe.name ? '!border-cyan/30 !bg-cyan/[0.06]' : ''"
        @click="recipe.error ? (error = recipe.error) : selectRecipe(recipe.name)"
      >
        <div class="min-w-0 flex-1">
          <p class="truncate font-mono text-[11px]" :class="recipe.error ? 'text-danger' : 'text-ink'">
            {{ recipe.name }}
          </p>
          <p class="font-mono text-[9px] uppercase tracking-[0.12em] text-faint">
            {{ recipe.error ? 'invalid recipe' : `${recipe.mode} · ${recipe.roles} roles` }}
          </p>
        </div>
        <span class="chip text-[9px]">{{ recipe.runtime }}</span>
      </button>
      <p v-if="!recipeList.length && !loading" class="px-2 py-4 text-center font-mono text-[10px] text-faint">
        No recipes yet.
      </p>
      <div class="divider my-2" />
      <button class="btn w-full" @click="newRecipe">
        <Plus class="h-3.5 w-3.5" /> Blank recipe
      </button>
    </aside>

    <!-- Editor -->
    <section class="panel overflow-hidden">
      <header class="flex flex-wrap items-center gap-3 border-b border-edge px-5 py-4">
        <div class="min-w-0">
          <p class="hud-label text-cyan/70">
            {{ isNew ? 'New recipe' : `Editing ${selected}` }}
          </p>
          <h3 class="truncate text-lg font-semibold tracking-tight">
            {{ draft?.metadata?.displayName || draft?.metadata?.name || 'Untitled' }}
          </h3>
        </div>
        <div class="ml-auto flex flex-wrap items-center gap-2">
          <span v-if="dirty" class="chip chip-warn">unsaved</span>
          <button class="btn" :disabled="!draft" @click="validateRecipe">
            <Check class="h-3.5 w-3.5" /> Validate
          </button>
          <button class="btn btn-primary" :disabled="saving || !draft" @click="saveRecipe">
            <Save class="h-3.5 w-3.5" :class="{ 'animate-pulse': saving }" /> Save
          </button>
          <button
            v-if="hasModel && selected"
            class="btn btn-violet"
            :disabled="dirty"
            title="Save changes before swapping models"
            @click="openSwap"
          >
            <ArrowRightLeft class="h-3.5 w-3.5" /> Swap model
          </button>
        </div>
      </header>

      <div class="flex items-center gap-1 border-b border-edge px-4 py-2">
        <button
          v-for="entry in [
            { key: 'structured', label: 'Structured' },
            { key: 'yaml', label: 'Raw YAML' },
            { key: 'preview', label: 'Preview' },
          ]"
          :key="entry.key"
          class="rounded-md px-3 py-1.5 font-mono text-[10px] uppercase tracking-[0.14em] transition"
          :class="tab === entry.key ? 'bg-cyan/10 text-cyan-soft' : 'text-faint hover:text-ink'"
          @click="switchTab(entry.key)"
        >
          {{ entry.label }}
        </button>
        <div class="ml-auto flex items-center gap-2">
          <button
            v-if="selected"
            class="btn"
            :class="deleteArmed === selected ? 'btn-danger' : ''"
            @click="deleteRecipe"
          >
            <Trash2 class="h-3.5 w-3.5" />
            {{ deleteArmed === selected ? 'Confirm delete' : 'Delete' }}
          </button>
          <button v-if="selected" class="btn" @click="openDuplicate">
            <Copy class="h-3.5 w-3.5" /> Duplicate
          </button>
        </div>
      </div>

      <div class="p-5">
        <div
          v-if="error"
          class="mb-4 flex items-center gap-2 rounded-md border border-danger/40 bg-danger/10 px-3 py-2"
        >
          <p class="font-mono text-[11px] text-danger">{{ error }}</p>
          <button class="ml-auto text-faint hover:text-ink" @click="error = null">
            <X class="h-3.5 w-3.5" />
          </button>
        </div>
        <div
          v-else-if="notice"
          class="mb-4 flex items-center gap-2 rounded-md border border-signal/30 bg-signal/[0.05] px-3 py-2"
        >
          <p class="font-mono text-[11px] text-signal">{{ notice }}</p>
          <button class="ml-auto text-faint hover:text-ink" @click="notice = null">
            <X class="h-3.5 w-3.5" />
          </button>
        </div>

        <RecipeForm
          v-if="draft && tab === 'structured'"
          :recipe="draft"
          :nodes="nodes"
          :disabled-name="!isNew"
        />

        <div v-else-if="tab === 'yaml'" class="space-y-3">
          <textarea
            v-model="yamlText"
            class="input h-[62vh] font-mono text-[11px] leading-relaxed"
            spellcheck="false"
          />
          <div class="flex items-center gap-2">
            <button class="btn" @click="applyYaml">Parse YAML</button>
            <span class="font-mono text-[10px] text-faint">
              Raw editing preserves every Compose key.
            </span>
          </div>
        </div>

        <div v-else class="space-y-4">
          <div v-if="!validation" class="panel-raised grid place-items-center p-10 text-center">
            <Check class="h-5 w-5 text-faint" />
            <p class="mt-2 text-sm text-dim">Validate the recipe to preview rendered Compose.</p>
            <button class="btn btn-primary mt-3" @click="validateRecipe">Validate now</button>
          </div>
          <div v-else class="space-y-4">
            <div class="grid gap-3 sm:grid-cols-3">
              <div class="panel-raised p-3">
                <p class="hud-label">Mode</p>
                <p class="hud-value mt-1">{{ validation.recipe.spec.mode }}</p>
              </div>
              <div class="panel-raised p-3">
                <p class="hud-label">Roles</p>
                <p class="hud-value mt-1">{{ validation.rendered.length }}</p>
              </div>
              <div class="panel-raised p-3">
                <p class="hud-label">Served model</p>
                <p class="hud-value mt-1">{{ validation.recipe.spec.model?.servedName || '—' }}</p>
              </div>
            </div>
            <div class="grid gap-4 xl:grid-cols-2">
              <article
                v-for="item in validation.rendered"
                :key="item.role"
                class="panel-raised overflow-hidden"
              >
                <header class="flex items-center gap-2 border-b border-edge px-3 py-2">
                  <span class="chip">{{ item.role }}</span>
                  <span class="font-mono text-[10px] text-faint">{{ item.node }}</span>
                  <span class="ml-auto font-mono text-[9px] text-cyan/70">
                    {{ item.configHash?.slice(0, 12) }}
                  </span>
                </header>
                <pre class="max-h-72 overflow-auto p-3 font-mono text-[10px] leading-relaxed text-dim">{{ composeYaml(item.compose) }}</pre>
              </article>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Model swap dialog -->
    <div
      v-if="swapOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="swapOpen = false"
    >
      <div class="panel w-full max-w-3xl p-6">
        <div class="flex items-center gap-2">
          <ArrowRightLeft class="h-4 w-4 text-violet" />
          <h3 class="text-base font-semibold">Model Swap</h3>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="swapOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <p class="mt-1 font-mono text-[10px] text-faint">
          Flags and roles are preserved; the diff is shown before applying.
        </p>
        <div class="mt-4 grid gap-3 sm:grid-cols-3">
          <div>
            <label class="hud-label">Repo</label>
            <input v-model="swapForm.repo" class="input mt-1" />
          </div>
          <div>
            <label class="hud-label">Served name</label>
            <input v-model="swapForm.servedName" class="input mt-1" />
          </div>
          <div>
            <label class="hud-label">Revision</label>
            <input v-model="swapForm.revision" class="input mt-1" placeholder="optional" />
          </div>
        </div>
        <p v-if="swapError" class="mt-3 font-mono text-[11px] text-danger">{{ swapError }}</p>
        <div v-if="swapPreview" class="mt-4">
          <YamlDiff :diff="swapPreview.diff" />
        </div>
        <div class="mt-5 flex justify-end gap-2">
          <button class="btn" @click="swapOpen = false">Cancel</button>
          <button class="btn btn-violet" :disabled="swapBusy" @click="previewSwap">
            <RefreshCw class="h-3.5 w-3.5" :class="{ 'animate-spin': swapBusy }" /> Preview diff
          </button>
          <button
            class="btn btn-primary"
            :disabled="swapBusy || !swapPreview"
            @click="applySwap"
          >
            <Check class="h-3.5 w-3.5" /> Apply swap
          </button>
        </div>
      </div>
    </div>

    <!-- Duplicate dialog -->
    <div
      v-if="duplicateOpen"
      class="fixed inset-0 z-[60] grid place-items-center bg-void/80 px-4 backdrop-blur-sm"
      @click.self="duplicateOpen = false"
    >
      <div class="panel w-full max-w-md p-6">
        <div class="flex items-center gap-2">
          <Copy class="h-4 w-4 text-cyan" />
          <h3 class="text-base font-semibold">Duplicate Recipe</h3>
          <button class="ml-auto rounded-md p-1 text-faint hover:text-ink" @click="duplicateOpen = false">
            <X class="h-4 w-4" />
          </button>
        </div>
        <label class="hud-label mt-4 block">New name</label>
        <input v-model="duplicateName" class="input mt-1" />
        <p v-if="duplicateError" class="mt-2 font-mono text-[11px] text-danger">{{ duplicateError }}</p>
        <div class="mt-4 flex justify-end gap-2">
          <button class="btn" @click="duplicateOpen = false">Cancel</button>
          <button class="btn btn-primary" :disabled="!duplicateName" @click="duplicateRecipe">
            Create copy
          </button>
        </div>
      </div>
    </div>
  </div>
</template>
