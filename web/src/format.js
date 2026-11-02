/** Formatting helpers shared by fleet views. */

const BYTE_UNITS = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']

export function formatBytes(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  let bytes = Number(value)
  if (bytes < 0) return '—'
  let index = 0
  while (bytes >= 1024 && index < BYTE_UNITS.length - 1) {
    bytes /= 1024
    index += 1
  }
  const digits = index === 0 ? 0 : bytes >= 100 ? 0 : bytes >= 10 ? 1 : 2
  return `${bytes.toFixed(digits)} ${BYTE_UNITS[index]}`
}

export function formatPercent(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return `${Number(value).toFixed(digits)}%`
}

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return new Intl.NumberFormat('en-GB').format(Number(value))
}

export function formatCompact(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return new Intl.NumberFormat('en-GB', {
    notation: 'compact',
    maximumFractionDigits: 1,
  }).format(Number(value))
}

export function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) {
    return '—'
  }
  let remaining = Math.max(0, Math.floor(Number(seconds)))
  if (remaining < 60) return `${remaining}s`
  const days = Math.floor(remaining / 86400)
  remaining %= 86400
  const hours = Math.floor(remaining / 3600)
  remaining %= 3600
  const minutes = Math.floor(remaining / 60)
  const secs = remaining % 60
  if (days) return `${days}d ${hours}h`
  if (hours) return `${hours}h ${minutes}m`
  return `${minutes}m ${secs}s`
}

export function formatRelative(timestamp, now = Date.now()) {
  if (timestamp === null || timestamp === undefined) return 'never'
  const value = Number(timestamp)
  const seconds = Math.max(0, Math.floor(now / 1000 - value))
  if (seconds < 5) return 'just now'
  if (seconds < 60) return `${seconds}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  return `${Math.floor(seconds / 86400)}d ago`
}

export function formatTime(timestamp) {
  if (timestamp === null || timestamp === undefined) return '—'
  const date = new Date(Number(timestamp) * 1000)
  return date.toLocaleTimeString('en-GB', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}

export function shortId(value, length = 8) {
  if (!value) return '—'
  return String(value).slice(0, length)
}

const STATUS_TONES = {
  loaded: 'chip-live',
  running: 'chip-live',
  succeeded: 'chip-live',
  healthy: 'chip-live',
  active: 'chip-live',
  pending: 'chip-warn',
  loading: 'chip-warn',
  starting: 'chip-warn',
  queued: 'chip-warn',
  unloaded: '',
  stopped: '',
  cancelled: 'chip-danger',
  failed: 'chip-danger',
  error: 'chip-danger',
}

export function statusTone(status) {
  return STATUS_TONES[String(status || '').toLowerCase()] ?? ''
}

export function statusLabel(status) {
  if (!status) return 'unknown'
  return String(status).replaceAll('_', ' ')
}

export function roleClassTone(roleClass) {
  return roleClass === 'heavy' ? 'text-violet-soft' : 'text-cyan-soft'
}

export function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value))
}
