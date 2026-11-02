/**
 * Typed fetch helpers for the spark-fleet controller API.
 *
 * Every helper returns parsed JSON, throws `ApiError` with the controller's
 * `detail` message, and attaches the session CSRF header to mutations when the
 * operator has logged in. Path segments are encoded individually so model repo
 * ids keep their `/` separators.
 */

const CSRF_KEY = 'spark_fleet_csrf'
const storage = typeof sessionStorage === 'undefined' ? null : sessionStorage

let csrfToken = storage ? storage.getItem(CSRF_KEY) : null

export class ApiError extends Error {
  constructor(message, { status = 0, detail = null, cause = null } = {}) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
    if (cause) this.cause = cause
  }
}

export function setCsrf(token) {
  csrfToken = token || null
  if (!storage) return
  if (csrfToken) storage.setItem(CSRF_KEY, csrfToken)
  else storage.removeItem(CSRF_KEY)
}

export function getCsrf() {
  return csrfToken
}

export function clearCsrf() {
  setCsrf(null)
}

function encodePath(value) {
  return String(value)
    .split('/')
    .map((part) => encodeURIComponent(part))
    .join('/')
}

function query(params = {}) {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === '') continue
    search.set(key, String(value))
  }
  const text = search.toString()
  return text ? `?${text}` : ''
}

async function request(path, { method = 'GET', body, headers = {}, signal } = {}) {
  const init = {
    method,
    credentials: 'same-origin',
    headers: { Accept: 'application/json', ...headers },
    signal,
  }
  if (body !== undefined) {
    init.headers['Content-Type'] = 'application/json'
    init.body = JSON.stringify(body)
  }
  if (!['GET', 'HEAD'].includes(method) && csrfToken) {
    init.headers['X-CSRF-Token'] = csrfToken
  }

  let response
  try {
    response = await fetch(path, init)
  } catch (error) {
    throw new ApiError('Controller unreachable', { cause: error })
  }

  const text = await response.text()
  let payload = null
  if (text) {
    try {
      payload = JSON.parse(text)
    } catch {
      payload = text
    }
  }

  if (!response.ok) {
    const detail =
      payload && typeof payload === 'object' && payload.detail
        ? String(payload.detail)
        : typeof payload === 'string' && payload
          ? payload
          : `Request failed (${response.status})`
    throw new ApiError(detail, { status: response.status, detail })
  }
  return payload
}

export const api = {
  health: () => request('/health'),

  async session() {
    const session = await request('/api/session')
    if (session && session.csrf) setCsrf(session.csrf)
    return session
  },

  async login(password) {
    const result = await request('/api/login', {
      method: 'POST',
      body: { password },
    })
    if (result && result.csrf) setCsrf(result.csrf)
    return result
  },

  async logout() {
    try {
      return await request('/api/logout', { method: 'POST' })
    } finally {
      clearCsrf()
    }
  },

  nodes: {
    list: () => request('/api/nodes'),
    create: (node) => request('/api/nodes', { method: 'POST', body: node }),
    update: (id, patch) =>
      request(`/api/nodes/${encodeURIComponent(id)}`, { method: 'PUT', body: patch }),
    remove: (id) =>
      request(`/api/nodes/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    check: (id) =>
      request(`/api/nodes/${encodeURIComponent(id)}/check`, { method: 'POST' }),
    containers: (id, params = {}) =>
      request(
        `/api/nodes/${encodeURIComponent(id)}/containers${query(params)}`,
      ),
    containerLogs: (id, containerId, tail = 200) =>
      request(
        `/api/nodes/${encodeURIComponent(id)}/containers/${encodeURIComponent(
          containerId,
        )}/logs${query({ tail })}`,
      ),
    containerLogStreamUrl: (id, containerId, tail = 200) =>
      `/api/nodes/${encodeURIComponent(id)}/containers/${encodeURIComponent(
        containerId,
      )}/logs/stream${query({ tail })}`,
  },

  recipes: {
    list: () => request('/api/recipes'),
    get: (name) => request(`/api/recipes/${encodeURIComponent(name)}`),
    put: (name, yaml) =>
      request(`/api/recipes/${encodeURIComponent(name)}`, {
        method: 'PUT',
        body: { yaml },
      }),
    remove: (name) =>
      request(`/api/recipes/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    validate: (recipe) =>
      request('/api/recipes/validate', { method: 'POST', body: recipe }),
    swapModel: (name, body) =>
      request(`/api/recipes/${encodeURIComponent(name)}/swap-model`, {
        method: 'POST',
        body,
      }),
  },

  deployments: {
    list: () => request('/api/deployments'),
    get: (id) => request(`/api/deployments/${encodeURIComponent(id)}`),
    create: (body) => request('/api/deployments', { method: 'POST', body }),
    remove: (id) =>
      request(`/api/deployments/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    load: (id, primary = false) =>
      request(`/api/deployments/${encodeURIComponent(id)}/load`, {
        method: 'POST',
        body: { primary },
      }),
    unload: (id) =>
      request(`/api/deployments/${encodeURIComponent(id)}/unload`, {
        method: 'POST',
      }),
    redeploy: (id, primary = false) =>
      request(`/api/deployments/${encodeURIComponent(id)}/redeploy`, {
        method: 'POST',
        body: { primary },
      }),
    setPrimary: (id) =>
      request(`/api/deployments/${encodeURIComponent(id)}/primary`, {
        method: 'POST',
      }),
    roleLogs: (id, role, tail = 200) =>
      request(
        `/api/deployments/${encodeURIComponent(id)}/roles/${encodeURIComponent(role)}/logs${query(
          { tail },
        )}`,
      ),
    roleLogStreamUrl: (id, role, tail = 200) =>
      `/api/deployments/${encodeURIComponent(id)}/roles/${encodeURIComponent(role)}/logs/stream${query(
        { tail },
      )}`,
  },

  models: {
    list: () => request('/api/models'),
    download: (body) =>
      request('/api/models/download', { method: 'POST', body }),
    jobs: (nodeId = null) =>
      request(`/api/models/jobs${query({ node_id: nodeId })}`),
    job: (nodeId, jobId) =>
      request(
        `/api/models/jobs/${encodeURIComponent(nodeId)}/${encodeURIComponent(jobId)}`,
      ),
    cancelJob: (nodeId, jobId) =>
      request(
        `/api/models/jobs/${encodeURIComponent(nodeId)}/${encodeURIComponent(jobId)}/cancel`,
        { method: 'POST' },
      ),
    remove: (repo, { nodeId, root, confirm = true } = {}) =>
      request(
        `/api/models/${encodePath(repo)}${query({
          node_id: nodeId,
          root,
          confirm,
        })}`,
        { method: 'DELETE' },
      ),
  },

  jobs: {
    list: (limit = 100) => request(`/api/jobs${query({ limit })}`),
    get: (id) => request(`/api/jobs/${encodeURIComponent(id)}`),
    cancel: (id) =>
      request(`/api/jobs/${encodeURIComponent(id)}/cancel`, { method: 'POST' }),
  },

  metrics: {
    node: (nodeId, params = {}) =>
      request(`/api/metrics/nodes/${encodeURIComponent(nodeId)}${query(params)}`),
    inference: (deploymentId, params = {}) =>
      request(
        `/api/metrics/inference/${encodeURIComponent(deploymentId)}${query(params)}`,
      ),
  },

  settings: {
    list: () => request('/api/settings'),
    put: (key, value) =>
      request(`/api/settings/${encodeURIComponent(key)}`, {
        method: 'PUT',
        body: { value },
      }),
  },

  import: {
    plan: (sources) =>
      request('/api/import/plan', { method: 'POST', body: { sources } }),
    apply: (sources) =>
      request('/api/import/apply', {
        method: 'POST',
        body: { sources, confirm: true },
      }),
  },

  gatewayModels: () => request('/v1/models'),
}

export default api
