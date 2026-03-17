# Compilation Cache

NebulaStream can cache compilation artifacts of compiled operator pipelines (Nautilus/MLIR backend). The goal is to
avoid repeating identical compilation work across query runs by storing the generated Nautilus “blob” on disk and loading
it on subsequent runs. This cache works internally per pipeline stage, not per whole query.

## Configuration

The compilation cache is controlled by the worker configuration:

- `worker.enable_compilation_cache = true`
- `worker.compilation_cache_dir = /tmp/nes-compilation-cache`

## Cache Key Generation (NebulaStream)

NebulaStream generates an explicit cache key string and passes it to Nautilus via engine options in
`CompilationCache::configureEngineOptionsForPipeline` (`nes-query-compiler/src/CompilationCache.cpp`), invoked from
`LowerToCompiledQueryPlanPhase::getStage`:

- `engine.Blob.CacheDir = <compilation_cache_dir>`
- `engine.Blob.CacheKey = <explicit_key>`

The explicit key is built from two main parts:

1. A **query seed** (`cacheKeySeed`) shared by all pipelines of a query.
2. A **stable pipeline ordinal** that disambiguates multiple pipelines within one query.

### 1) Query Seed (`cacheKeySeed`)

`CompilationCache::createCacheKeySeed(const PhysicalPlan&)` requires a precomputed seed attached by the optimizer.
`QueryOptimizer::optimize(...)` computes this seed from a canonical serialization of the optimized logical plan
(after join-type and memory-layout decisions, before lowering to physical operators) via
`OptimizedLogicalPlanSignatureUtil::create(...)`.

The canonical optimized logical-plan seed includes:

- codegen-relevant query-execution settings that are not encoded in the logical plan itself:
  - execution mode
  - operator buffer size
  - page size
  - number of records per key
  - max number of buckets
- a recursive, id-free serialization of the optimized logical operator tree, including:
  - operator type, schemas, traits, and config
  - logical functions and aggregation functions
  - window metadata and other serialized config values
  - child structure using stable structural ordinals instead of generated operator ids
- canonicalized source/sink descriptors that strip runtime-only identity fields:
  - sources keep source type, schema, and parser config
  - sinks keep sink type, schema, and input format when present
- trait serialization excludes `OutputOriginIds`, whose ids are not stable across runs and are not codegen-relevant

If no precomputed seed is attached, compilation-cache key generation now fails fast instead of silently falling back
to a physical-plan signature.

The resulting seed is stored on the query compiler’s `CompilationCache` instance (owned by `QueryCompiler`) and used as
the query-level input for all per-pipeline cache keys.

### 2) Stable Pipeline Ordinal (`o=...`)

`CompilationCache` assigns a stable ordinal (`o=<n>`) the first time a `Pipeline` is encountered during lowering. This
disambiguates pipelines within the same query without relying on non-stable identifiers.

### Final Explicit Key Format

The explicit key string has the shape:

`nes:auto:q=<cacheKeySeed>:o=<stageOrdinal>`

Note: this string is not used as a filename. Nautilus hashes it to generate a stable file path.

## Storage Format

Nautilus stores the cached artifact in `engine.Blob.CacheDir`. The on-disk filename is a **hash** derived from:

- a cache format/version string (to invalidate old cache layouts),
- compilation backend options (MLIR-related flags),
- the explicit cache key, and
- the wrapper function target type name (to avoid collisions across different wrapper function types).

The primary file is an MLIR bytecode blob:

- `<hash>.mlirbc`

Depending on the compilation, Nautilus may also write sidecar files next to the blob:

- `<hash>.mlirbc.sym` (symbol map for JIT proxy functions)
- `<hash>.mlirbc.constptrmap` (metadata for relocating constant pointers when safe/possible)

To support concurrent compilation, Nautilus writes to a unique temporary file and publishes it with an atomic rename.

## Cache Hits and Fallbacks

On a cache hit, Nautilus attempts to load the blob and can skip tracing/IR generation and most compilation work.
If loading fails, Nautilus falls back to recompiling.

## Caveats

- The cache is intended for reuse within the same build of NebulaStream + Nautilus.
- The cache key also includes a binary fingerprint derived from the current executable path, file size, and modification
  time.
  This avoids using stale artifacts produced by a different binary.
- The cache directory is not automatically cleaned; remove it to evict entries.
- Changes in SQL, operator parameters, schemas, memory layouts, pipeline structure/order, or compiler/backend options
  are expected to produce cache misses.
