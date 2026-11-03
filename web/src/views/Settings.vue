<script setup>
import { onMounted, ref } from 'vue'
import {
  Clock,
  KeyRound,
  LogOut,
  Radio,
  RotateCcw,
  Save,
  Settings as SettingsIcon,
  Shield,
} from 'lucide-vue-next'
import { api } from '../api'

const config = ref(null)
const session = ref(null)
const settings = ref({})
const loading = ref(true)
const error = ref(null)
const notice = ref(null)
const saving = ref(false)
const form = ref({
  rollback_policy: 'restore',
  metrics_retention_seconds: 86400,
  metrics_sample_interval_seconds: 10,
  node_poll_interval_seconds: 15,
})

async function load() {
  loading.value = true
  error.value = null
  try {
    const [configResponse, sessionResponse] = await Promise.all([
      api.config(),
      api.session(),
    ])
    config.value = configResponse
    session.value = sessionResponse
    try {
      const settingsResponse = await api.settings.list()
      settings.value = settingsResponse.settings || {}
    } catch {
      settings.value = {}
    }
    form.value = {
      rollback_policy: settings.value.rollback_policy || configResponse.rollbackPolicy,
      metrics_retention_seconds:
        Number(settings.value.metrics_retention_seconds) ||
        configResponse.metricsRetentionSeconds,
      metrics_sample_interval_seconds:
        Number(settings.value.metrics_sample_interval_seconds) ||
        configResponse.metricsSampleIntervalSeconds,
      node_poll_interval_seconds:
        Number(settings.value.node_poll_interval_seconds) ||
        configResponse.nodePollIntervalSeconds,
    }
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    loading.value = false
  }
}

async function save() {
  const retention = Number(form.value.metrics_retention_seconds)
  const sample = Number(form.value.metrics_sample_interval_seconds)
  const poll = Number(form.value.node_poll_interval_seconds)
  if (retention < 60 || retention > 2592000) {
    error.value = 'Retention must be between 60 and 2592000 seconds'
    return
  }
  if (sample < 1 || sample > 3600 || poll < 1 || poll > 3600) {
    error.value = 'Intervals must be between 1 and 3600 seconds'
    return
  }
  saving.value = true
  error.value = null
  try {
    for (const [key, value] of Object.entries(form.value)) {
      await api.settings.put(key, String(value))
    }
    notice.value = 'Settings stored — restart the controller to apply them'
    await load()
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    saving.value = false
  }
}

async function logout() {
  try {
    await api.logout()
    session.value = await api.session()
    notice.value = 'Operator session closed'
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

onMounted(load)
</script>

<template>
  <div class="space-y-5">
    <header class="flex flex-wrap items-end gap-3">
      <div>
        <p class="hud-label text-cyan/70">Control Plane</p>
        <h3 class="mt-1 text-lg font-semibold tracking-tight">Settings</h3>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <span class="chip" :class="session?.authenticated ? 'chip-live' : ''">
          <Shield class="h-3 w-3" />
          {{ session?.authenticated ? 'operator authenticated' : 'LAN open' }}
        </span>
        <button v-if="session?.authenticated" class="btn" @click="logout">
          <LogOut class="h-3.5 w-3.5" /> Logout
        </button>
        <button class="btn btn-primary" :disabled="saving" @click="save">
          <Save class="h-3.5 w-3.5" :class="{ 'animate-pulse': saving }" />
          Save settings
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

    <div class="grid gap-5 xl:grid-cols-2">
      <section class="panel p-5">
        <header class="flex items-center gap-2">
          <RotateCcw class="h-4 w-4 text-cyan" />
          <div>
            <p class="hud-label text-cyan/70">Lifecycle</p>
            <h4 class="text-sm font-semibold">Rollback &amp; Retention</h4>
          </div>
        </header>
        <div class="divider my-4" />
        <label class="hud-label">Rollback policy</label>
        <select v-model="form.rollback_policy" class="input mt-1">
          <option value="restore">restore — reload displaced deployments</option>
          <option value="leave-down">leave-down — stop everything</option>
        </select>
        <div class="mt-4 grid gap-3 sm:grid-cols-2">
          <div>
            <label class="hud-label flex items-center gap-1.5">
              <Clock class="h-3 w-3" /> Retention (seconds)
            </label>
            <input
              v-model.number="form.metrics_retention_seconds"
              class="input mt-1"
              type="number"
              min="60"
              max="2592000"
            />
          </div>
          <div>
            <label class="hud-label flex items-center gap-1.5">
              <Radio class="h-3 w-3" /> Metrics interval (s)
            </label>
            <input
              v-model.number="form.metrics_sample_interval_seconds"
              class="input mt-1"
              type="number"
              min="1"
              max="3600"
            />
          </div>
          <div>
            <label class="hud-label">Node poll interval (s)</label>
            <input
              v-model.number="form.node_poll_interval_seconds"
              class="input mt-1"
              type="number"
              min="1"
              max="3600"
            />
          </div>
        </div>
        <p class="mt-3 font-mono text-[10px] text-faint">
          Persisted overrides are read once at controller startup; restart to apply.
        </p>
      </section>

      <section class="panel p-5">
        <header class="flex items-center gap-2">
          <SettingsIcon class="h-4 w-4 text-violet" />
          <div>
            <p class="hud-label text-violet/70">Effective Runtime</p>
            <h4 class="text-sm font-semibold">Controller Configuration</h4>
          </div>
        </header>
        <div class="divider my-4" />
        <div v-if="config" class="grid gap-2 sm:grid-cols-2">
          <div class="panel-raised p-3">
            <p class="hud-label">Rollback</p>
            <p class="hud-value mt-1 text-[12px]">{{ config.rollbackPolicy }}</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Retention</p>
            <p class="hud-value mt-1 text-[12px]">{{ config.metricsRetentionSeconds }}s</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Sample interval</p>
            <p class="hud-value mt-1 text-[12px]">{{ config.metricsSampleIntervalSeconds }}s</p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Node poll</p>
            <p class="hud-value mt-1 text-[12px]">{{ config.nodePollIntervalSeconds }}s</p>
          </div>
        </div>
        <div class="mt-4 rounded-md border border-edge bg-void/40 px-3 py-2">
          <p class="hud-label flex items-center gap-1.5"><KeyRound class="h-3 w-3" /> Agent token</p>
          <p class="mt-1 font-mono text-[10px] text-dim">
            Shared bearer token is supplied by the environment and never rendered here.
          </p>
        </div>
        <div v-if="config" class="mt-2 rounded-md border border-edge bg-void/40 px-3 py-2">
          <p class="hud-label">Gateway</p>
          <p class="mt-1 font-mono text-[10px] text-cyan-soft">
            {{ config.gateway.modelsPath }} · active alias
            {{ config.gateway.activeAlias }}
          </p>
        </div>
      </section>

      <section class="panel p-5 xl:col-span-2">
        <header class="flex items-center gap-2">
          <Shield class="h-4 w-4 text-signal" />
          <div>
            <p class="hud-label text-signal/70">Access</p>
            <h4 class="text-sm font-semibold">Authentication</h4>
          </div>
        </header>
        <div class="divider my-4" />
        <div class="grid gap-3 sm:grid-cols-3">
          <div class="panel-raised p-3">
            <p class="hud-label">UI login</p>
            <p class="mt-1">
              <span class="chip" :class="config?.authEnabled ? 'chip-live' : ''">
                {{ config?.authEnabled ? 'enabled' : 'disabled' }}
              </span>
            </p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Session</p>
            <p class="mt-1">
              <span class="chip" :class="session?.authenticated ? 'chip-live' : ''">
                {{ session?.authenticated ? 'authenticated' : 'anonymous' }}
              </span>
            </p>
          </div>
          <div class="panel-raised p-3">
            <p class="hud-label">Transport</p>
            <p class="mt-1">
              <span class="chip chip-warn">plain HTTP · LAN only</span>
            </p>
          </div>
        </div>
        <div class="mt-4 flex items-center gap-2">
          <router-link to="/login" class="btn">
            <KeyRound class="h-3.5 w-3.5" />
            Open login screen
          </router-link>
          <span class="font-mono text-[10px] text-faint">
            TLS and public exposure require an authenticated reverse proxy.
          </span>
        </div>
      </section>
    </div>
  </div>
</template>
