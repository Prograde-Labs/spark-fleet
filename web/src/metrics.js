/** Derived telemetry helpers shared by the dashboard and node cards. */

export function gpuMemoryKnown(gpu) {
  return (
    gpu.memory_total_bytes !== null &&
    gpu.memory_total_bytes !== undefined &&
    gpu.memory_used_bytes !== null &&
    gpu.memory_used_bytes !== undefined
  )
}

export function gpuResident(gpu) {
  if (gpu.resident === null || gpu.resident === undefined) return null
  return Number(gpu.resident) >= 1
}

export function cacheCapacityKnown(metrics) {
  return Number(metrics.cache_total_bytes || 0) > 0
}

export function cachePercent(metrics) {
  if (!cacheCapacityKnown(metrics)) return 0
  return Math.min(
    100,
    (Number(metrics.cache_bytes || 0) / Number(metrics.cache_total_bytes)) * 100,
  )
}

const reported = (value) => value !== null && value !== undefined

export function cacheKnown(metrics) {
  return (
    Number(metrics.cache_total_bytes || 0) > 0 &&
    reported(metrics.cache_free_bytes)
  )
}

export function cacheBytesKnown(metrics) {
  return reported(metrics.cache_bytes)
}

export function cacheHeadline(aggregate) {
  return aggregate.reported > 0 ? aggregate.used : null
}

export function cacheCaption(aggregate) {
  if (
    aggregate.reported === 0 &&
    aggregate.capacity === 0 &&
    aggregate.free === 0
  ) {
    return 'unavailable'
  }
  return aggregate.complete ? 'complete' : 'partial'
}

export function groupGpus(metrics) {
  const groups = new Map()
  for (const [key, value] of Object.entries(metrics || {})) {
    const match = /^gpu\.(\d+)\.(.+)$/.exec(key)
    if (!match) continue
    const index = Number(match[1])
    if (!groups.has(index)) groups.set(index, { index })
    groups.get(index)[match[2]] = value
  }
  return [...groups.values()].sort((a, b) => a.index - b.index)
}

export function cacheAggregate(metricsByNode) {
  const entries = Object.values(metricsByNode || {}).filter(Boolean)
  const hasCacheFields = (metrics) =>
    reported(metrics.cache_bytes) ||
    reported(metrics.cache_total_bytes) ||
    reported(metrics.cache_free_bytes)
  const reportedNodes = entries.filter((metrics) =>
    reported(metrics.cache_bytes),
  ).length
  const used = entries.reduce(
    (sum, metrics) =>
      reported(metrics.cache_bytes) ? sum + Number(metrics.cache_bytes) : sum,
    0,
  )
  const capacity = entries.reduce(
    (sum, metrics) =>
      reported(metrics.cache_total_bytes)
        ? sum + Number(metrics.cache_total_bytes)
        : sum,
    0,
  )
  const free = entries.reduce(
    (sum, metrics) =>
      reported(metrics.cache_free_bytes)
        ? sum + Number(metrics.cache_free_bytes)
        : sum,
    0,
  )
  const complete =
    reportedNodes > 0 &&
    entries.every((metrics) => {
      if (metrics.fetchFailed) return false
      if (!hasCacheFields(metrics)) return true
      return (
        reported(metrics.cache_bytes) &&
        reported(metrics.cache_total_bytes) &&
        Number(metrics.cache_total_bytes) > 0 &&
        reported(metrics.cache_free_bytes)
      )
    })
  return { used, capacity, free, complete, reported: reportedNodes }
}
