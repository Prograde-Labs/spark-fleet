<script setup>
import { computed, ref } from 'vue'
import {
  Boxes,
  FileText,
  Play,
  RotateCw,
  Square,
  Star,
  Trash2,
  TriangleAlert,
} from 'lucide-vue-next'
import { formatRelative, shortId, statusLabel, statusTone } from '../format'

const props = defineProps({
  item: { type: Object, required: true },
  nodes: { type: Object, default: () => ({}) },
  busy: { type: String, default: null },
  job: { type: Object, default: null },
})

const emit = defineEmits(['load', 'unload', 'redeploy', 'primary', 'delete', 'logs'])

const confirmUnload = ref(false)
const confirmDelete = ref(false)

const deployment = computed(() => props.item.deployment)
const roles = computed(() => props.item.roles || [])
const loaded = computed(() => deployment.value.status === 'loaded')
const pending = computed(() =>
  ['pending', 'loading', 'starting'].includes(deployment.value.status),
)
const isBusy = computed(() => props.busy === deployment.value.id)
const canBePrimary = computed(
  () => loaded.value && deployment.value.mode === 'deployment',
)

function nodeName(nodeId) {
  return props.nodes[nodeId]?.display_name || nodeId
}

function nodeIp(nodeId) {
  return props.nodes[nodeId]?.lan_ip || '—'
}

function askUnload() {
  if (!confirmUnload.value) {
    confirmUnload.value = true
    return
  }
  confirmUnload.value = false
  emit('unload', deployment.value)
}

function askDelete() {
  if (!confirmDelete.value) {
    confirmDelete.value = true
    return
  }
  confirmDelete.value = false
  emit('delete', deployment.value)
}
</script>

<template>
  <article class="panel relative overflow-hidden">
    <div
      class="absolute inset-y-0 left-0 w-1"
      :class="
        deployment.status === 'loaded'
          ? 'bg-signal'
          : deployment.status === 'failed'
            ? 'bg-danger'
            : 'bg-edgeBright'
      "
    />

    <div class="p-5 pl-6">
      <header class="flex flex-wrap items-start gap-3">
        <div class="min-w-0 flex-1">
          <div class="flex flex-wrap items-center gap-2">
            <h3 class="truncate text-base font-semibold tracking-tight">
              {{ deployment.name }}
            </h3>
            <span class="chip" :class="statusTone(deployment.status)">
              {{ statusLabel(deployment.status) }}
            </span>
            <span v-if="item.stale" class="chip chip-warn">
              <TriangleAlert class="h-3 w-3" />
              stale recipe
            </span>
            <span v-if="deployment.is_primary" class="chip chip-live">
              <Star class="h-3 w-3 fill-signal" />
              spark-active
            </span>
          </div>
          <p class="mt-1 font-mono text-[10px] uppercase tracking-[0.14em] text-faint">
            {{ deployment.mode }} · recipe {{ deployment.recipe_name }} · id
            {{ shortId(deployment.id) }}
          </p>
        </div>

        <button
          v-if="canBePrimary"
          class="rounded-md border p-1.5 transition"
          :class="
            deployment.is_primary
              ? 'border-signal/40 bg-signal/10 text-signal'
              : 'border-edge text-faint hover:border-signal/40 hover:text-signal'
          "
          :disabled="isBusy || deployment.is_primary"
          title="Make spark-active"
          @click="emit('primary', deployment)"
        >
          <Star class="h-4 w-4" :class="{ 'fill-signal': deployment.is_primary }" />
        </button>
      </header>

      <div class="mt-4 grid gap-2 sm:grid-cols-2">
        <div
          v-for="role in roles"
          :key="role.role_name"
          class="flex items-center gap-2 rounded-md border border-edge/70 bg-void/40 px-3 py-2"
        >
          <Boxes
            class="h-3.5 w-3.5"
            :class="role.class === 'heavy' ? 'text-violet' : 'text-cyan'"
          />
          <div class="min-w-0 flex-1">
            <div class="flex items-center gap-2">
              <span class="truncate font-mono text-[11px] text-ink">{{ role.role_name }}</span>
              <span class="chip" :class="statusTone(role.status)">
                {{ statusLabel(role.status) }}
              </span>
            </div>
            <p class="font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
              {{ nodeName(role.node_id) }} · {{ nodeIp(role.node_id) }} ·
              {{ role.class }}
            </p>
          </div>
          <button
            class="rounded-md border border-edge p-1 text-faint transition hover:border-cyan/40 hover:text-cyan disabled:opacity-30"
            :disabled="!role.container_id"
            title="Role logs"
            @click="emit('logs', role)"
          >
            <FileText class="h-3.5 w-3.5" />
          </button>
        </div>
        <p
          v-if="!roles.length"
          class="col-span-full rounded-md border border-dashed border-edge px-3 py-2 font-mono text-[10px] text-faint"
        >
          No role rows yet — load the deployment to materialize containers.
        </p>
      </div>

      <div
        v-if="job"
        class="mt-3 flex items-center gap-2 rounded-md border border-cyan/20 bg-cyan/[0.04] px-3 py-2"
      >
        <RotateCw class="h-3.5 w-3.5 animate-spin text-cyan" />
        <span class="font-mono text-[10px] uppercase tracking-[0.14em] text-cyan-soft">
          {{ job.phase || job.status || 'working' }}
        </span>
        <span class="truncate font-mono text-[10px] text-dim">{{ job.message || '' }}</span>
        <span class="ml-auto font-mono text-[9px] text-faint">{{ shortId(job.id, 6) }}</span>
      </div>

      <footer class="mt-4 flex flex-wrap items-center gap-2 border-t border-edge/70 pt-3">
        <button
          v-if="!loaded && !pending"
          class="btn btn-primary"
          :disabled="isBusy"
          @click="emit('load', deployment)"
        >
          <Play class="h-3.5 w-3.5" />
          Load
        </button>
        <button
          v-if="loaded"
          class="btn btn-violet"
          :disabled="isBusy"
          @click="emit('redeploy', deployment)"
        >
          <RotateCw class="h-3.5 w-3.5" :class="{ 'animate-spin': isBusy }" />
          Redeploy
        </button>
        <button
          v-if="loaded"
          class="btn"
          :class="confirmUnload ? 'btn-danger' : ''"
          :disabled="isBusy"
          @click="askUnload"
        >
          <Square class="h-3.5 w-3.5" />
          {{ confirmUnload ? 'Confirm unload' : 'Unload' }}
        </button>
        <button
          v-if="!loaded"
          class="btn ml-auto"
          :class="confirmDelete ? 'btn-danger' : ''"
          :disabled="isBusy"
          @click="askDelete"
        >
          <Trash2 class="h-3.5 w-3.5" />
          {{ confirmDelete ? 'Confirm delete' : 'Delete' }}
        </button>
        <span v-if="confirmUnload || confirmDelete" class="hud-label ml-auto">
          Click again to confirm
        </span>
      </footer>

      <div class="mt-2 flex flex-wrap items-center gap-x-4 font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
        <span>Updated {{ formatRelative(deployment.updated_at) }}</span>
        <span v-if="deployment.loaded_at">Loaded {{ formatRelative(deployment.loaded_at) }}</span>
        <span v-if="deployment.error" class="text-danger normal-case">
          {{ deployment.error }}
        </span>
      </div>
    </div>
  </article>
</template>
