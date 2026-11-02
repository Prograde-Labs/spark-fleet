<script setup>
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { ArrowLeft, Cpu, KeyRound, Lock, ShieldCheck, ShieldOff } from 'lucide-vue-next'
import { api } from '../api'

const router = useRouter()
const session = ref(null)
const password = ref('')
const busy = ref(false)
const error = ref(null)

const enabled = ref(false)
const authenticated = ref(false)

async function loadSession() {
  try {
    session.value = await api.session()
    enabled.value = Boolean(session.value.enabled)
    authenticated.value = Boolean(session.value.authenticated)
  } catch (exc) {
    error.value = exc.message || String(exc)
  }
}

async function submit() {
  busy.value = true
  error.value = null
  try {
    await api.login(password.value)
    password.value = ''
    await loadSession()
    if (authenticated.value) router.push('/')
  } catch (exc) {
    error.value = exc.message || String(exc)
  } finally {
    busy.value = false
  }
}

onMounted(loadSession)
</script>

<template>
  <main class="relative grid min-h-screen place-items-center px-4 py-10">
    <div class="pointer-events-none absolute inset-0 bg-grid-fade opacity-25" aria-hidden="true" />
    <div class="relative w-full max-w-md">
      <router-link
        to="/"
        class="mb-4 inline-flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.16em] text-faint transition hover:text-cyan"
      >
        <ArrowLeft class="h-3.5 w-3.5" />
        Back to fleet
      </router-link>

      <section class="panel relative overflow-hidden p-7">
        <div class="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-cyan/60 via-violet/60 to-transparent" />
        <div class="flex items-center gap-3">
          <div class="grid h-11 w-11 place-items-center rounded-lg border border-cyan/40 bg-cyan/10 shadow-glow-cyan">
            <Cpu class="h-5 w-5 text-cyan" />
          </div>
          <div>
            <p class="hud-label text-cyan/80">Spark Fleet</p>
            <h1 class="text-lg font-semibold tracking-tight">Operator Access</h1>
          </div>
        </div>

        <div class="divider my-5" />

        <div v-if="!session" class="flex items-center gap-2 font-mono text-[11px] text-faint">
          <span class="h-1.5 w-1.5 animate-pulseSoft rounded-full bg-cyan" />
          Checking controller session…
        </div>

        <template v-else>
          <div v-if="!enabled" class="space-y-3">
            <div class="flex items-center gap-2">
              <ShieldOff class="h-4 w-4 text-warning" />
              <p class="text-sm font-medium">Login is disabled</p>
            </div>
            <p class="text-sm text-dim">
              This controller trusts the LAN boundary. Set
              <span class="font-mono text-[11px] text-cyan-soft">SPARK_FLEET_UI_PASSWORD</span>
              and
              <span class="font-mono text-[11px] text-cyan-soft">SPARK_FLEET_SESSION_SECRET</span>
              to require an operator session.
            </p>
            <router-link to="/" class="btn btn-primary w-full">
              <ArrowLeft class="h-3.5 w-3.5" /> Continue to fleet
            </router-link>
          </div>

          <div v-else-if="authenticated" class="space-y-3">
            <div class="flex items-center gap-2">
              <ShieldCheck class="h-4 w-4 text-signal" />
              <p class="text-sm font-medium">Operator session active</p>
            </div>
            <p class="text-sm text-dim">
              Mutations carry the session CSRF token. You can continue to mission
              control or end the session from settings.
            </p>
            <router-link to="/" class="btn btn-primary w-full">
              Continue to fleet
            </router-link>
          </div>

          <form v-else class="space-y-4" @submit.prevent="submit">
            <div class="flex items-center gap-2">
              <Lock class="h-4 w-4 text-violet" />
              <p class="text-sm font-medium">Enter operator password</p>
            </div>
            <div>
              <label class="hud-label">Password</label>
              <input
                v-model="password"
                class="input mt-1"
                type="password"
                autocomplete="current-password"
                placeholder="••••••••"
              />
            </div>
            <p v-if="error" class="font-mono text-[11px] text-danger">{{ error }}</p>
            <button class="btn btn-primary w-full" :disabled="busy || !password">
              <KeyRound class="h-3.5 w-3.5" :class="{ 'animate-pulse': busy }" />
              Authenticate
            </button>
            <p class="font-mono text-[9px] uppercase tracking-[0.16em] text-faint">
              Session cookie · HttpOnly · SameSite=strict
            </p>
          </form>
        </template>
      </section>

      <p class="mt-4 text-center font-mono text-[9px] uppercase tracking-[0.18em] text-faint">
        spark-fleet // trusted LAN boundary
      </p>
    </div>
  </main>
</template>
