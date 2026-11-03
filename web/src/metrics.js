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
  const contributors = Object.values(metricsByNode || {}).filter((metrics) =>
    reported(metrics.cache_bytes),
  )
  const used = contributors.reduce(
    (sum, metrics) => sum + Number(metrics.cache_bytes || 0),
    0,
  )
  const capacity = contributors.reduce(
    (sum, metrics) => sum + Number(metrics.cache_total_bytes || 0),
    0,
  )
  const free = contributors.reduce(
    (sum, metrics) => sum + Number(metrics.cache_free_bytes || 0),
    0,
  )
  const complete =
    contributors.length > 0 &&
    contributors.every(
      (metrics) =>
        reported(metrics.cache_total_bytes) &&
        reported(metrics.cache_free_bytes),
    )
  return { used, capacity, free, complete }
}
