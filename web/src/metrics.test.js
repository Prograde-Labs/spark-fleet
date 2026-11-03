import assert from 'node:assert/strict'
import test from 'node:test'

import {
  cacheAggregate,
  cacheCapacityKnown,
  cachePercent,
  gpuMemoryKnown,
  gpuResident,
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

test('cachePercent scales cache bytes to filesystem capacity', () => {
  assert.equal(cachePercent({ cache_bytes: 250, cache_total_bytes: 1000 }), 25)
  assert.equal(cachePercent({ cache_bytes: 2000, cache_total_bytes: 1000 }), 100)
  assert.equal(cachePercent({ cache_bytes: 250 }), 0)
})

test('cacheAggregate excludes nodes without cache bytes', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_total_bytes: 4000, cache_free_bytes: 1000 },
  })

  assert.deepEqual(aggregate, {
    used: 10,
    capacity: 1000,
    free: 250,
    complete: true,
  })
})

test('cacheAggregate treats a reported zero as a contributor', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 0 },
  })

  assert.equal(aggregate.complete, false)
  assert.equal(aggregate.used, 10)
})

test('cacheAggregate is incomplete when a contributor lacks capacity', () => {
  const aggregate = cacheAggregate({
    a: { cache_bytes: 10, cache_total_bytes: 1000, cache_free_bytes: 250 },
    b: { cache_bytes: 5 },
  })

  assert.equal(aggregate.complete, false)
})

test('cacheAggregate with no contributors is empty and incomplete', () => {
  assert.deepEqual(cacheAggregate({}), {
    used: 0,
    capacity: 0,
    free: 0,
    complete: false,
  })
})
