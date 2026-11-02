/**
 * Live fleet state fed by the controller SSE stream.
 *
 * The store keeps a bounded event log plus keyed node/job/deployment/metric
 * snapshots. Named SSE events are dispatched with `addEventListener`; if the
 * stream drops, it reconnects with capped exponential backoff until `stop()` is
 * called.
 */

import { reactive, readonly } from 'vue'

const MAX_EVENTS = 250
const RETRY_BASE_MS = 1000
const RETRY_MAX_MS = 30000

const state = reactive({
  connected: false,
  connecting: false,
  stopped: true,
  lastEventAt: null,
  error: null,
  attempts: 0,
  events: [],
  nodes: {},
  jobs: {},
  deployments: {},
  metrics: {},
})

let source = null
let retryTimer = null

function record(type, data) {
  const entry = {
    type,
    data,
    at: Date.now(),
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
  }
  state.events.unshift(entry)
  if (state.events.length > MAX_EVENTS) state.events.length = MAX_EVENTS
  state.lastEventAt = entry.at
  state.error = null
}

function ingest(type, data) {
  record(type, data)
  if (!data || typeof data !== 'object') return

  if (type === 'node.health' && data.node_id) {
    state.nodes[data.node_id] = {
      ...(state.nodes[data.node_id] || {}),
      healthy: Boolean(data.healthy),
      error: data.error || null,
      at: Date.now(),
    }
  } else if (type === 'metrics.sample' && data.node_id) {
    state.metrics[data.node_id] = {
      ...(state.metrics[data.node_id] || {}),
      ...data,
      at: Date.now(),
    }
  } else if (type === 'job' && data.id) {
    state.jobs[data.id] = {
      ...(state.jobs[data.id] || {}),
      ...data,
      at: Date.now(),
    }
  } else if (type === 'deployment.reconciled' && data.deployment_id) {
    state.deployments[data.deployment_id] = {
      ...(state.deployments[data.deployment_id] || {}),
      ...data,
      at: Date.now(),
    }
  }
}

function scheduleReconnect() {
  if (state.stopped) return
  const delay = Math.min(RETRY_BASE_MS * 2 ** state.attempts, RETRY_MAX_MS)
  state.attempts += 1
  clearTimeout(retryTimer)
  retryTimer = setTimeout(open, delay)
}

function open() {
  if (state.stopped) return
  state.connecting = true
  try {
    source = new EventSource('/events')
  } catch (error) {
    state.error = String(error)
    state.connecting = false
    scheduleReconnect()
    return
  }

  source.onopen = () => {
    state.connected = true
    state.connecting = false
    state.attempts = 0
    state.error = null
  }

  source.onerror = () => {
    state.connected = false
    state.connecting = false
    state.error = 'Event stream disconnected'
    if (source) {
      source.close()
      source = null
    }
    scheduleReconnect()
  }

  source.onmessage = (event) => {
    ingest('message', parse(event.data))
  }

  for (const type of ['job', 'node.health', 'metrics.sample', 'deployment.reconciled']) {
    source.addEventListener(type, (event) => {
      ingest(type, parse(event.data))
    })
  }
}

function parse(raw) {
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

export function startEvents() {
  if (!state.stopped) return
  state.stopped = false
  open()
}

export function stopEvents() {
  state.stopped = true
  state.connected = false
  state.connecting = false
  state.attempts = 0
  clearTimeout(retryTimer)
  retryTimer = null
  if (source) {
    source.close()
    source = null
  }
}

export function clearEvents() {
  state.events.splice(0, state.events.length)
}

export function noteDeployment(deployment) {
  if (!deployment || !deployment.id) return
  state.deployments[deployment.id] = {
    ...(state.deployments[deployment.id] || {}),
    ...deployment,
    at: Date.now(),
  }
}

export function noteJob(job) {
  if (!job || !job.id) return
  state.jobs[job.id] = { ...(state.jobs[job.id] || {}), ...job, at: Date.now() }
}

export const eventState = readonly(state)

export default eventState
