<script setup>
import { computed } from 'vue'
import { Plus, Trash2 } from 'lucide-vue-next'

const props = defineProps({
  recipe: { type: Object, required: true },
  nodes: { type: Array, default: () => [] },
  disabledName: { type: Boolean, default: false },
})

const spec = computed(() => props.recipe.spec || (props.recipe.spec = {}))
const roles = computed(() => spec.value.roles || (spec.value.roles = []))
const prepare = computed(() => spec.value.prepare || (spec.value.prepare = []))

const runtimeOptions = ['vllm', 'sglang', 'llamacpp', 'tensorfold', 'comfyui', 'custom']
const readinessOptions = ['running', 'container-health', 'http', 'tcp', 'log-pattern']

function ensureModel() {
  if (!spec.value.model) {
    spec.value.model = {
      repo: '',
      servedName: '',
      revision: null,
      extraRepos: [],
    }
  }
}

function removeModel() {
  spec.value.model = null
}

function ensureGateway() {
  if (!spec.value.gateway) spec.value.gateway = { aliases: [] }
}

function removeGateway() {
  spec.value.gateway = null
}

function addRole() {
  const heavy = roles.value.filter((role) => role.class === 'heavy').length
  roles.value.push({
    name: heavy === 0 ? 'head' : `worker-${heavy}`,
    node: props.nodes[0]?.id || '',
    class: 'heavy',
    startOrder: heavy === 0 ? 20 : 10,
    inferencePort: heavy === 0 ? 8888 : null,
    readiness:
      heavy === 0
        ? { type: 'http', path: '/v1/models', timeoutSeconds: 3600 }
        : { type: 'running', timeoutSeconds: 3600 },
    service: { image: '', network_mode: 'host', volumes: [] },
  })
}

function removeRole(index) {
  roles.value.splice(index, 1)
}

function addPrepare() {
  prepare.value.push({
    name: `prepare-${prepare.value.length + 1}`,
    node: props.nodes[0]?.id || '',
    image: '',
    command: [],
    volumes: [],
    environment: {},
    timeoutSeconds: 600,
  })
}

function removePrepare(index) {
  prepare.value.splice(index, 1)
}

function listText(list) {
  return Array.isArray(list) ? list.join('\n') : ''
}

function setList(target, key, text) {
  target[key] = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
}

function envText(environment) {
  if (!environment || typeof environment !== 'object') return ''
  return Object.entries(environment)
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')
}

function setEnv(target, key, text) {
  const environment = {}
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const separator = trimmed.indexOf('=')
    if (separator === -1) continue
    environment[trimmed.slice(0, separator)] = trimmed.slice(separator + 1)
  }
  target[key] = environment
}

function aliasesText() {
  return (spec.value.gateway?.aliases || []).join(', ')
}

function setAliases(text) {
  spec.value.gateway.aliases = text
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function setReadinessType(role, type) {
  role.readiness = { ...(role.readiness || {}), type }
}
</script>

<template>
  <div class="space-y-5">
    <section class="panel-raised p-4">
      <p class="hud-label text-cyan/70">Identity</p>
      <div class="mt-3 grid gap-3 sm:grid-cols-2">
        <div>
          <label class="hud-label">Name</label>
          <input
            v-model="recipe.metadata.name"
            class="input mt-1"
            :disabled="disabledName"
            placeholder="demo-glm"
          />
        </div>
        <div>
          <label class="hud-label">Display name</label>
          <input v-model="recipe.metadata.displayName" class="input mt-1" placeholder="GLM 5.3 Flash" />
        </div>
        <div class="sm:col-span-2">
          <label class="hud-label">Description</label>
          <input v-model="recipe.metadata.description" class="input mt-1" placeholder="Optional operator note" />
        </div>
      </div>
    </section>

    <section class="panel-raised p-4">
      <p class="hud-label text-violet/70">Runtime</p>
      <div class="mt-3 grid gap-3 sm:grid-cols-2">
        <div>
          <label class="hud-label">Mode</label>
          <select v-model="spec.mode" class="input mt-1">
            <option value="deployment">deployment</option>
            <option value="service">service</option>
          </select>
        </div>
        <div>
          <label class="hud-label">Runtime</label>
          <select v-model="spec.runtime" class="input mt-1">
            <option v-for="runtime in runtimeOptions" :key="runtime" :value="runtime">
              {{ runtime }}
            </option>
          </select>
        </div>
        <div>
          <label class="hud-label">Topology preset</label>
          <select v-model="spec.topology.preset" class="input mt-1">
            <option value="single">single</option>
            <option value="custom">custom</option>
            <option value="tensorfold">tensorfold</option>
          </select>
        </div>
        <div>
          <label class="hud-label">Master port</label>
          <input v-model.number="spec.topology.masterPort" class="input mt-1" type="number" min="1" max="65535" />
        </div>
      </div>
    </section>

    <section class="panel-raised p-4">
      <div class="flex items-center gap-2">
        <p class="hud-label text-cyan/70">Model &amp; Gateway</p>
        <button v-if="!spec.model" class="btn ml-auto" @click="ensureModel">Add model</button>
        <button v-else class="btn ml-auto" @click="removeModel">Remove model</button>
      </div>
      <div v-if="spec.model" class="mt-3 grid gap-3 sm:grid-cols-2">
        <div>
          <label class="hud-label">Repo</label>
          <input v-model="spec.model.repo" class="input mt-1" placeholder="org/model" />
        </div>
        <div>
          <label class="hud-label">Served name</label>
          <input v-model="spec.model.servedName" class="input mt-1" placeholder="Demo-Model-B" />
        </div>
        <div>
          <label class="hud-label">Revision</label>
          <input v-model="spec.model.revision" class="input mt-1" placeholder="main / commit" />
        </div>
        <div>
          <label class="hud-label">Context length (tokens)</label>
          <input
            v-model.number="spec.model.contextLength"
            class="input mt-1"
            type="number"
            min="1"
            placeholder="auto"
          />
        </div>        <div>
          <label class="hud-label">Extra repos (one per line)</label>
          <textarea
            class="input mt-1 h-20"
            :value="listText(spec.model.extraRepos)"
            @input="setList(spec.model, 'extraRepos', $event.target.value)"
          />
        </div>
      </div>
      <div class="mt-3 flex items-center gap-2">
        <p class="hud-label">Gateway aliases</p>
        <button v-if="!spec.gateway" class="btn ml-auto" @click="ensureGateway">Add gateway</button>
        <button v-else class="btn ml-auto" @click="removeGateway">Remove gateway</button>
      </div>
      <input
        v-if="spec.gateway"
        class="input mt-2"
        :value="aliasesText()"
        placeholder="demo-glm, Demo-Model-B"
        @input="setAliases($event.target.value)"
      />
    </section>

    <section class="panel-raised p-4">
      <div class="flex items-center gap-2">
        <p class="hud-label text-violet/70">Roles</p>
        <button class="btn ml-auto" @click="addRole">
          <Plus class="h-3.5 w-3.5" /> Add role
        </button>
      </div>
      <div class="mt-3 space-y-4">
        <article
          v-for="(role, index) in roles"
          :key="index"
          class="rounded-lg border border-edge bg-void/40 p-3"
        >
          <div class="flex items-center gap-2">
            <span class="font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
              Role {{ index + 1 }}
            </span>
            <button class="ml-auto rounded-md p-1 text-faint hover:text-danger" @click="removeRole(index)">
              <Trash2 class="h-3.5 w-3.5" />
            </button>
          </div>
          <div class="mt-2 grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            <div>
              <label class="hud-label">Name</label>
              <input v-model="role.name" class="input mt-1" />
            </div>
            <div>
              <label class="hud-label">Node</label>
              <select v-model="role.node" class="input mt-1">
                <option v-for="node in nodes" :key="node.id" :value="node.id">
                  {{ node.display_name }} ({{ node.id }})
                </option>
              </select>
            </div>
            <div>
              <label class="hud-label">Class</label>
              <select v-model="role.class" class="input mt-1">
                <option value="heavy">heavy</option>
                <option value="light">light</option>
              </select>
            </div>
            <div>
              <label class="hud-label">Start order</label>
              <input v-model.number="role.startOrder" class="input mt-1" type="number" min="0" />
            </div>
            <div>
              <label class="hud-label">Inference port</label>
              <input v-model.number="role.inferencePort" class="input mt-1" type="number" min="1" max="65535" />
            </div>
            <div>
              <label class="hud-label">Readiness</label>
              <select
                :value="role.readiness?.type || 'running'"
                class="input mt-1"
                @change="setReadinessType(role, $event.target.value)"
              >
                <option v-for="type in readinessOptions" :key="type" :value="type">
                  {{ type }}
                </option>
              </select>
            </div>
            <div v-if="['http', 'tcp', 'log-pattern'].includes(role.readiness?.type)">
              <label class="hud-label">
                {{ role.readiness.type === 'log-pattern' ? 'Pattern' : 'Path' }}
              </label>
              <input
                v-if="role.readiness.type !== 'log-pattern'"
                v-model="role.readiness.path"
                class="input mt-1"
                placeholder="/v1/models"
              />
              <input v-else v-model="role.readiness.pattern" class="input mt-1" placeholder="listening" />
            </div>
            <div v-if="['http', 'tcp'].includes(role.readiness?.type)">
              <label class="hud-label">Readiness port</label>
              <input v-model.number="role.readiness.port" class="input mt-1" type="number" />
            </div>
            <div>
              <label class="hud-label">Timeout (s)</label>
              <input v-model.number="role.readiness.timeoutSeconds" class="input mt-1" type="number" min="1" />
            </div>
            <div>
              <label class="hud-label">Image</label>
              <input v-model="role.service.image" class="input mt-1" placeholder="ghcr.io/org/img:tag" />
            </div>
            <div>
              <label class="hud-label">Network mode</label>
              <input v-model="role.service.network_mode" class="input mt-1" placeholder="host" />
            </div>
            <div>
              <label class="hud-label">GPUs</label>
              <input v-model="role.service.gpus" class="input mt-1" placeholder="all" />
            </div>
            <div>
              <label class="hud-label">IPC</label>
              <input v-model="role.service.ipc" class="input mt-1" placeholder="host" />
            </div>
            <div>
              <label class="hud-label">Shared memory</label>
              <input v-model="role.service.shm_size" class="input mt-1" placeholder="32gb" />
            </div>
            <div>
              <label class="hud-label">Volumes (one per line)</label>
              <textarea
                class="input mt-1 h-16"
                :value="listText(role.service.volumes)"
                @input="setList(role.service, 'volumes', $event.target.value)"
              />
            </div>
            <div>
              <label class="hud-label">Ports (one per line)</label>
              <textarea
                class="input mt-1 h-16"
                :value="listText(role.service.ports)"
                @input="setList(role.service, 'ports', $event.target.value)"
              />
            </div>
            <div>
              <label class="hud-label">Environment (KEY=VALUE)</label>
              <textarea
                class="input mt-1 h-20"
                :value="envText(role.service.environment)"
                @input="setEnv(role.service, 'environment', $event.target.value)"
              />
            </div>
            <div class="sm:col-span-2">
              <label class="hud-label">Command (one arg per line)</label>
              <textarea
                class="input mt-1 h-20"
                :value="listText(role.service.command)"
                @input="setList(role.service, 'command', $event.target.value)"
              />
            </div>
          </div>
        </article>
        <p v-if="!roles.length" class="rounded-md border border-dashed border-edge px-3 py-3 font-mono text-[10px] text-faint">
          A recipe needs at least one role.
        </p>
      </div>
    </section>

    <section class="panel-raised p-4">
      <div class="flex items-center gap-2">
        <p class="hud-label text-cyan/70">Prepare jobs</p>
        <button class="btn ml-auto" @click="addPrepare">
          <Plus class="h-3.5 w-3.5" /> Add prepare
        </button>
      </div>
      <div class="mt-3 space-y-3">
        <article
          v-for="(item, index) in prepare"
          :key="index"
          class="rounded-lg border border-edge bg-void/40 p-3"
        >
          <div class="flex items-center gap-2">
            <span class="font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
              Prepare {{ index + 1 }}
            </span>
            <button class="ml-auto rounded-md p-1 text-faint hover:text-danger" @click="removePrepare(index)">
              <Trash2 class="h-3.5 w-3.5" />
            </button>
          </div>
          <div class="mt-2 grid gap-3 sm:grid-cols-2">
            <div>
              <label class="hud-label">Name</label>
              <input v-model="item.name" class="input mt-1" />
            </div>
            <div>
              <label class="hud-label">Node</label>
              <select v-model="item.node" class="input mt-1">
                <option v-for="node in nodes" :key="node.id" :value="node.id">
                  {{ node.display_name }}
                </option>
              </select>
            </div>
            <div>
              <label class="hud-label">Image</label>
              <input v-model="item.image" class="input mt-1" />
            </div>
            <div>
              <label class="hud-label">Timeout (s)</label>
              <input v-model.number="item.timeoutSeconds" class="input mt-1" type="number" min="1" />
            </div>
            <div class="sm:col-span-2">
              <label class="hud-label">Command (one arg per line)</label>
              <textarea
                class="input mt-1 h-16"
                :value="listText(item.command)"
                @input="setList(item, 'command', $event.target.value)"
              />
            </div>
            <div>
              <label class="hud-label">Volumes</label>
              <textarea
                class="input mt-1 h-16"
                :value="listText(item.volumes)"
                @input="setList(item, 'volumes', $event.target.value)"
              />
            </div>
            <div>
              <label class="hud-label">Environment (KEY=VALUE)</label>
              <textarea
                class="input mt-1 h-16"
                :value="envText(item.environment)"
                @input="setEnv(item, 'environment', $event.target.value)"
              />
            </div>
          </div>
        </article>
      </div>
    </section>
  </div>
</template>
