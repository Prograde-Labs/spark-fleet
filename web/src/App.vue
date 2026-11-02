<script setup>
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from './api'
import { formatRelative } from './format'
import { eventState, startEvents, stopEvents } from './stores/events'
import {
  Activity,
  Boxes,
  ChevronRight,
  Cpu,
  Database,
  DownloadCloud,
  FileCode2,
  LayoutDashboard,
  LogIn,
  Menu,
  Server,
  Settings,
  X,
} from 'lucide-vue-next'

const route = useRoute()
const router = useRouter()
const mobileOpen = ref(false)
const now = ref(new Date())
const session = ref({ enabled: false, authenticated: false })
let timer = null

const navGroups = [
  {
    label: 'Operations',
    items: [
      { to: '/', label: 'Fleet Overview', code: 'OPS-01', icon: LayoutDashboard },
      { to: '/deployments', label: 'Deployments', code: 'OPS-02', icon: Boxes },
      { to: '/recipes', label: 'Recipes', code: 'OPS-03', icon: FileCode2 },
      { to: '/models', label: 'Model Vault', code: 'OPS-04', icon: Database },
    ],
  },
  {
    label: 'Fleet',
    items: [
      { to: '/nodes', label: 'Node Registry', code: 'OPS-05', icon: Server },
      { to: '/activity', label: 'Activity Log', code: 'OPS-06', icon: Activity },
      { to: '/import', label: 'Import', code: 'OPS-07', icon: DownloadCloud },
    ],
  },
  {
    label: 'System',
    items: [{ to: '/settings', label: 'Settings', code: 'OPS-08', icon: Settings }],
  },
]

const bare = computed(() => route.meta?.layout === 'bare')
const title = computed(() => route.meta?.title || 'Fleet')
const code = computed(() => route.meta?.code || 'OPS-00')

const clock = computed(() =>
  now.value.toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }),
)
const date = computed(() =>
  now.value.toLocaleDateString('en-GB', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }),
)

onMounted(async () => {
  startEvents()
  timer = setInterval(() => {
    now.value = new Date()
  }, 1000)
  try {
    session.value = await api.session()
  } catch {
    session.value = { enabled: false, authenticated: false, offline: true }
  }
})

onBeforeUnmount(() => {
  stopEvents()
  if (timer) clearInterval(timer)
})

router.afterEach(() => {
  mobileOpen.value = false
})
</script>

<template>
  <router-view v-if="bare" />

  <div v-else class="relative flex min-h-screen">
    <!-- Ambient scan line -->
    <div
      class="pointer-events-none fixed inset-x-0 top-0 z-0 h-40 animate-scan bg-gradient-to-b from-cyan/[0.04] to-transparent"
      aria-hidden="true"
    />

    <!-- Mobile overlay -->
    <div
      v-if="mobileOpen"
      class="fixed inset-0 z-40 bg-void/70 backdrop-blur-sm lg:hidden"
      @click="mobileOpen = false"
    />

    <!-- Sidebar -->
    <aside
      class="fixed inset-y-0 left-0 z-50 flex w-[268px] flex-col border-r border-edge bg-abyss/95 transition-transform duration-300 lg:translate-x-0"
      :class="mobileOpen ? 'translate-x-0' : '-translate-x-full'"
    >
      <div class="flex items-center gap-3 border-b border-edge px-5 py-5">
        <div
          class="relative grid h-10 w-10 place-items-center rounded-lg border border-cyan/40 bg-cyan/10 shadow-glow-cyan"
        >
          <Cpu class="h-5 w-5 text-cyan" />
          <span
            class="absolute -right-1 -top-1 h-2 w-2 animate-pulseSoft rounded-full bg-signal"
          />
        </div>
        <div class="min-w-0">
          <p class="font-mono text-[10px] uppercase tracking-hud text-cyan/80">
            DGX Spark
          </p>
          <h1 class="truncate text-lg font-semibold tracking-tight">Spark Fleet</h1>
        </div>
        <button
          class="ml-auto rounded-md p-1 text-faint hover:text-ink lg:hidden"
          aria-label="Close navigation"
          @click="mobileOpen = false"
        >
          <X class="h-4 w-4" />
        </button>
      </div>

      <nav class="flex-1 space-y-6 overflow-y-auto px-3 py-5">
        <div v-for="group in navGroups" :key="group.label" class="space-y-1">
          <p class="px-3 pb-2 font-mono text-[9px] uppercase tracking-hud text-faint">
            {{ group.label }}
          </p>
          <router-link
            v-for="item in group.items"
            :key="item.to"
            :to="item.to"
            class="group flex items-center gap-3 rounded-lg border border-transparent px-3 py-2.5 text-sm text-dim transition hover:border-edge hover:bg-raised/60 hover:text-ink"
            active-class="!border-cyan/30 !bg-cyan/[0.07] !text-ink shadow-glow-cyan"
          >
            <component :is="item.icon" class="h-4 w-4 text-faint transition group-hover:text-cyan" />
            <span class="flex-1">{{ item.label }}</span>
            <span class="font-mono text-[9px] tracking-[0.14em] text-faint">
              {{ item.code }}
            </span>
            <ChevronRight
              class="h-3.5 w-3.5 opacity-0 transition group-hover:opacity-60"
            />
          </router-link>
        </div>
      </nav>

      <div class="border-t border-edge px-5 py-4">
        <div class="flex items-center justify-between">
          <span class="hud-label">Link</span>
          <span class="chip chip-live">
            <span class="h-1.5 w-1.5 animate-pulseSoft rounded-full bg-signal" />
            LAN
          </span>
        </div>
        <div class="mt-3 grid grid-cols-2 gap-2 font-mono text-[10px] text-faint">
          <div class="rounded-md border border-edge bg-void/50 px-2 py-1.5">
            <p class="text-[9px] uppercase tracking-[0.16em]">Controller</p>
            <p class="text-ink">:8091</p>
          </div>
          <div class="rounded-md border border-edge bg-void/50 px-2 py-1.5">
            <p class="text-[9px] uppercase tracking-[0.16em]">Gateway</p>
            <p class="text-ink">/v1</p>
          </div>
        </div>
        <router-link
          to="/login"
          class="mt-3 flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.14em] text-faint transition hover:text-cyan"
        >
          <LogIn class="h-3.5 w-3.5" />
          {{ session.authenticated ? 'Operator Online' : 'Operator Login' }}
        </router-link>
      </div>
    </aside>

    <!-- Main column -->
    <div class="flex min-w-0 flex-1 flex-col lg:pl-[268px]">
      <header
        class="sticky top-0 z-30 border-b border-edge bg-void/80 backdrop-blur-md"
      >
        <div class="flex items-center gap-4 px-4 py-3 lg:px-8">
          <button
            class="rounded-md border border-edge bg-raised p-2 text-dim hover:text-ink lg:hidden"
            aria-label="Open navigation"
            @click="mobileOpen = true"
          >
            <Menu class="h-4 w-4" />
          </button>

          <div class="min-w-0">
            <div class="flex items-center gap-2">
              <span class="hud-label text-cyan/80">{{ code }}</span>
              <span class="h-1 w-1 rounded-full bg-edgeBright" />
              <span class="hud-label">Mission Control</span>
            </div>
            <h2 class="truncate text-lg font-semibold tracking-tight">{{ title }}</h2>
          </div>

          <div class="ml-auto flex items-center gap-3">
            <div class="hidden text-right sm:block">
              <p class="font-mono text-sm tabular-nums text-ink">{{ clock }}</p>
              <p class="hud-label">{{ date }}</p>
            </div>
            <div class="hidden h-9 w-px bg-edge sm:block" />
            <div class="chip" :class="eventState.connected ? 'chip-live' : 'chip-warn'">
              <span
                class="h-1.5 w-1.5 rounded-full"
                :class="eventState.connected ? 'animate-pulseSoft bg-signal' : 'bg-warning'"
              />
              {{ eventState.connected ? 'Live link' : 'Standby' }}
            </div>
          </div>
        </div>
        <div class="glow-line" />
      </header>

      <main class="relative flex-1 px-4 py-6 lg:px-8 lg:py-8">
        <router-view v-slot="{ Component }">
          <transition
            enter-active-class="transition duration-200 ease-out"
            enter-from-class="opacity-0 translate-y-1"
            enter-to-class="opacity-100 translate-y-0"
            mode="out-in"
          >
            <component :is="Component" />
          </transition>
        </router-view>
      </main>

      <footer class="border-t border-edge px-4 py-3 lg:px-8">
        <div class="flex flex-wrap items-center gap-x-6 gap-y-1 font-mono text-[10px] uppercase tracking-[0.16em] text-faint">
          <span>spark-fleet // v1</span>
          <span class="hidden sm:inline">LAN-only control plane</span>
          <span class="ml-auto flex items-center gap-2">
            <span class="h-1.5 w-1.5 rounded-full bg-violet" />
            {{
              eventState.lastEventAt
                ? `Telemetry ${formatRelative(eventState.lastEventAt, now.getTime())}`
                : 'Awaiting telemetry'
            }}
          </span>
        </div>
      </footer>
    </div>
  </div>
</template>
