import assert from 'node:assert/strict'
import test from 'node:test'

import {
  cacheAggregate,
  cacheBytesKnown,
  cacheCaption,
  cacheCapacityKnown,
  cacheHeadline,
  cacheKnown,
  cachePercent,
  gpuMemoryKnown,
  gpuResident,
  groupGpus,
} from './metrics.js'

test('gpuResident is tri-state', () => {
  assert.equal(gpuResident({ resident: 1 }), true)
  assert.equal(gpuResident({ resident: 1.0 }), true)
  assert.equal(gpuResident({ resident: 0 }), false)
  assert.equal(gpuResident({ resident: null }), null)
  assert.equal(gpuResident({}), null)
})

test('gpuMemoryKnown requires both memory values', () => {
  assert.equal(gpuMemoryKnown({ memory_total_bytes: 1000, memory_used_bytes: 500 }), true)
  assert.equal(gpuMemoryKnown({ memory_total_bytes: 1000, memory_used_bytes: 0 }), true)
  assert.equal(gpuMemoryKnown({ memory_total_bytes: null, memory_used_bytes: 500 }), false)
  assert.equal(gpuMemoryKnown({ memory_total_bytes: 1000 }), false)
  assert.equal(gpuMemoryKnown({}), false)
})

test('cacheCapacityKnown ignores zero and missing totals', () => {
  assert.equal(cacheCapacityKnown({ cache_total_bytes: 1000 }), true)
  assert.equal(cacheCapacityKnown({ cache_total_bytes: 0 }), false)
  assert.equal(cacheCapacityKnown({}), false)
})

test('cacheKnown requires capacity and free space', () => {
  assert.equal(cacheKnown({ cache_total_bytes: 1000, cache_free_bytes: 250 }), true)
  assert.equal(cacheKnown({ cache_total_bytes: 1000, cache_free_bytes: 0 }), true)
  assert.equal(cacheKnown({ cache_total_bytes: 1000 }), false)
  assert.equal(cacheKnown({ cache_total_bytes: 1000, cache_free_bytes: null }), false)
  assert.equal(cacheKnown({ cache_total_bytes: 0, cache_free_bytes: 0 }), false)
  assert.equal(cacheKnown({}), false)
})

test('cacheBytesKnown accepts a reported zero', () => {
  assert.equal(cacheBytesKnown({ cache_bytes: 10 }), true)
  assert.equal(cacheBytesKnown({ cache_bytes: 0 }), true)
  assert.equal(cacheBytesKnown({ cache_bytes: null }), false)
  assert.equal(cacheBytesKnown({}), false)
})

test('cachePercent scales cache bytes to filesystem capacity', () => {
  assert.equal(cachePercent({ cache_bytes: 250, cache_total_bytes: 1000 }), 25)
  assert.equal(cachePercent({ cache_bytes: 2000, cache_total_bytes: 1000 }), 100)
  assert.equal(cachePercent({ cache_bytes: 250 }), 0)
})

test('cacheAggregate includes capacity from a node without cache bytes', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_total_bytes: 4000, cache_free_bytes: 1000 },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 5000,
    free: 1250,
    complete: false,
    reported: 1,
  })
})

test('cacheAggregate ignores a node with no cache telemetry', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { host_total_bytes: 8000 },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 1000,
    free: 250,
    complete: true,
    reported: 1,
  })
})

test('cacheAggregate is incomplete when a node fetch failed', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { fetchFailed: true },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 1000,
    free: 250,
    complete: false,
    reported: 1,
  })
})

test('cacheAggregate is incomplete when a failed fetch retains cache fields', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: {
      cache_bytes: 5,
      cache_total_bytes: 4000,
      cache_free_bytes: 1000,
      fetchFailed: true,
    },
  })

  assert.equal(aggregate.complete, false)
  assert.equal(aggregate.reported, 2)
  assert.equal(aggregate.used, 15)
})

test('cacheAggregate includes a zero-byte contributor with capacity', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 0, cache_total_bytes: 4000, cache_free_bytes: 1000 },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 5000,
    free: 1250,
    complete: true,
    reported: 2,
  })
})

test('cacheAggregate sums multiple complete contributors', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 5, cache_total_bytes: 4000, cache_free_bytes: 1000 },
  })

  assert.deepEqual(aggregate, {
    used: 15,
    capacity: 5000,
    free: 1250,
    complete: true,
    reported: 2,
  })
})

test('cacheAggregate treats a zero-byte contributor without capacity as incomplete', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 0 },
  })

  assert.equal(aggregate.complete, false)
  assert.equal(aggregate.used, 10)
  assert.equal(aggregate.reported, 2)
})

test('cacheAggregate is incomplete when a contributor lacks capacity', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 5 },
  })

  assert.equal(aggregate.complete, false)
})

test('cacheAggregate is incomplete when a contributor lacks free space', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 5, cache_total_bytes: 4000 },
  })

  assert.equal(aggregate.complete, false)
})

test('cacheAggregate is incomplete with zero capacity', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 0, cache_total_bytes: 0, cache_free_bytes: 0 },
  })

  assert.equal(aggregate.complete, false)
})

test('cacheAggregate with no nodes is empty and incomplete', () => {
  assert.deepEqual(cacheAggregate({}), {
    used: 0,
    capacity: 0,
    free: 0,
    complete: false,
    reported: 0,
  })
})

test('cacheAggregate without an argument is empty and incomplete', () => {
  assert.deepEqual(cacheAggregate(), {
    used: 0,
    capacity: 0,
    free: 0,
    complete: false,
    reported: 0,
  })
})

test('cacheAggregate ignores null node entries', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: null,
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 1000,
    free: 250,
    complete: true,
    reported: 1,
  })
})

test('cacheAggregate sums a free-only node without completing', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_free_bytes: 500 },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 1000,
    free: 750,
    complete: false,
    reported: 1,
  })
})

test('cacheHeadline hides a zero-byte aggregate that was never reported', () => {
  assert.equal(cacheHeadline(cacheAggregate({})), null)
  assert.equal(
    cacheHeadline(cacheAggregate({ a: { cache_bytes: 0, cache_total_bytes: 0 } })),
    0,
  )
  assert.equal(
    cacheHeadline(
      cacheAggregate({ a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 } }),
    ),
    10,
  )
})

test('cacheCaption distinguishes complete, partial, and unavailable', () => {
  assert.equal(cacheCaption(cacheAggregate({})), 'unavailable')
  assert.equal(
    cacheCaption(
      cacheAggregate({ a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 } }),
    ),
    'complete',
  )
  assert.equal(
    cacheCaption(
      cacheAggregate({ a: { cache_bytes: 10, cache_total_bytes: 1000 } }),
    ),
    'partial',
  )
  assert.equal(
    cacheCaption(
      cacheAggregate({ a: { cache_total_bytes: 1000, cache_free_bytes: 250 } }),
    ),
    'partial',
  )
})

test('groupGpus sorts by index and groups fields', () => {
  const gpus = groupGpus({
    'gpu.1.utilization_percent': 20,
    'gpu.1.memory_total_bytes': 2000,
    'gpu.0.utilization_percent': 90,
    'host_total_bytes': 1000,
  })

  assert.deepEqual(gpus, [
    { index: 0, utilization_percent: 90 },
    { index: 1, utilization_percent: 20, memory_total_bytes: 2000 },
  ])
})

test('groupGpus ignores malformed keys', () => {
  const gpus = groupGpus({
    'gpu.0.utilization_percent': 90,
    'gpu.abc.utilization_percent': 10,
    'gpu.0': 5,
    'gpu..temperature_celsius': 60,
  })

  assert.deepEqual(gpus, [{ index: 0, utilization_percent: 90 }])
})

test('groupGpus without metrics is empty', () => {
  assert.deepEqual(groupGpus(), [])
  assert.deepEqual(groupGpus({}), [])
})
