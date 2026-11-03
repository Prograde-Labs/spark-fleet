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

export function cacheAggregate(metricsByNode) {
  const entries = Object.values(metricsByNode || {})
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
    entries.length > 0 &&
    entries.every(
      (metrics) =>
        reported(metrics.cache_bytes) &&
        reported(metrics.cache_total_bytes) &&
        Number(metrics.cache_total_bytes) > 0 &&
        reported(metrics.cache_free_bytes),
    )
  return { used, capacity, free, complete }
}
