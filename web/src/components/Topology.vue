<script setup>
import { computed } from 'vue'
import { Boxes, Cpu, Network, Radio, Server, Star, Wifi } from 'lucide-vue-next'

const props = defineProps({
  nodes: { type: Array, default: () => [] },
  deployments: { type: Array, default: () => [] },
  primaryId: { type: String, default: null },
  health: { type: Object, default: () => ({}) },
})

const loaded = computed(() =>
  props.deployments.filter((item) => item.deployment.status === 'loaded'),
)

function deploymentsFor(nodeId) {
  return loaded.value.filter((item) =>
    (item.roles || []).some((role) => role.node_id === nodeId),
  )
}

function nodeTone(node) {
  if (!node.enabled) return { label: 'Disabled', tone: '' }
  const health = props.health[node.id]
  if (health && typeof health.healthy === 'boolean') {
    return health.healthy
      ? { label: 'Online', tone: 'chip-live' }
      : { label: 'Unreachable', tone: 'chip-danger' }
  }
  return { label: 'Unverified', tone: 'chip-warn' }
}
</script>

<template>
  <section class="panel relative overflow-hidden p-5">
    <header class="flex flex-wrap items-center gap-3">
      <div>
        <p class="hud-label text-cyan/70">Fabric Topology</p>
        <h3 class="mt-1 text-base font-semibold tracking-tight">Controller → Agents</h3>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <span class="chip">
          <Network class="h-3 w-3" />
          {{ nodes.length }} nodes
        </span>
        <span class="chip chip-live">
          <Radio class="h-3 w-3" />
          {{ loaded.length }} live
        </span>
      </div>
    </header>

    <div class="mt-5 grid gap-5 lg:grid-cols-[230px_1fr]">
      <div class="panel-raised relative self-start overflow-hidden p-4">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-cyan/50 to-violet/50" />
        <div class="flex items-center gap-2">
          <div class="grid h-8 w-8 place-items-center rounded-md border border-cyan/40 bg-cyan/10">
            <Cpu class="h-4 w-4 text-cyan" />
          </div>
          <div>
            <p class="text-sm font-semibold">spark-fleet</p>
            <p class="font-mono text-[9px] uppercase tracking-[0.16em] text-faint">
              Controller · 192.0.2.239:8091
            </p>
          </div>
        </div>
        <div class="mt-3 space-y-1.5 font-mono text-[10px] text-dim">
          <div class="flex justify-between">
            <span class="text-faint">Gateway</span>
            <span>/v1 · spark-active</span>
          </div>
          <div class="flex justify-between">
            <span class="text-faint">Docker socket</span>
            <span class="text-signal">not mounted</span>
          </div>
          <div class="flex justify-between">
            <span class="text-faint">State</span>
            <span>SQLite</span>
          </div>
        </div>
        <div class="mt-3 flex items-center gap-2">
          <span class="h-1.5 w-1.5 animate-pulseSoft rounded-full bg-signal" />
          <span class="hud-label">LAN trusted boundary</span>
        </div>
      </div>

      <div class="relative space-y-3 border-l border-edge/70 pl-5">
        <span
          class="pointer-events-none absolute inset-y-2 -left-px w-px bg-gradient-to-b from-cyan/40 via-edgeBright to-violet/40"
          aria-hidden="true"
        />
        <article
          v-for="node in nodes"
          :key="node.id"
          class="relative rounded-lg border border-edge bg-void/40 px-4 py-3 transition hover:border-edgeBright"
        >
          <span
            class="pointer-events-none absolute -left-5 top-1/2 h-px w-5 bg-edgeBright/70"
            aria-hidden="true"
          />
          <div class="flex flex-wrap items-center gap-2">
            <Server class="h-3.5 w-3.5 text-cyan" />
            <span class="text-sm font-medium">{{ node.display_name }}</span>
            <span class="chip" :class="nodeTone(node).tone">
              <span class="h-1.5 w-1.5 rounded-full bg-current" />
              {{ nodeTone(node).label }}
            </span>
            <span class="ml-auto font-mono text-[9px] uppercase tracking-[0.14em] text-faint">
              {{ node.id }}
            </span>
          </div>
          <div class="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 font-mono text-[10px] text-faint">
            <span class="flex items-center gap-1">
              <Wifi class="h-3 w-3" /> {{ node.lan_ip }}
            </span>
            <span class="flex items-center gap-1">
              <Network class="h-3 w-3" /> {{ node.fabric_ip }}
            </span>
          </div>
          <div class="mt-2 flex flex-wrap gap-1.5">
            <span
              v-for="item in deploymentsFor(node.id)"
              :key="item.deployment.id"
              class="chip"
              :class="item.deployment.is_primary ? 'chip-live' : 'chip-warn'"
            >
              <Star v-if="item.deployment.is_primary" class="h-3 w-3" />
              <Boxes v-else class="h-3 w-3" />
              {{ item.deployment.name }}
            </span>
            <span v-if="!deploymentsFor(node.id).length" class="hud-label">
              idle
            </span>
          </div>
        </article>

        <p v-if="!nodes.length" class="py-6 text-center font-mono text-[11px] text-faint">
          No agents registered.
        </p>
      </div>
    </div>
  </section>
</template>
