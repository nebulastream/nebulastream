# Variable-sized buffers via size classes

> **Scope note (after review #1792).** This document was originally much broader. Following the review it
> is narrowed to one question: **how do we serve a variable-sized buffer from the pool instead of the
> unpooled path**, plus the two consumers that need it (right-sized emit, the network receive path). The
> two showstoppers it previously bundled are handled elsewhere and are *prerequisites*, not part of this
> design: bounding the unpooled path is already merged (#1701/#1702, keyseven123), and terminating a query
> on genuine exhaustion is tracked in #1700. Adaptive source provisioning (#1713) and disk spilling
> (#1699) are dropped from scope — see Non-Goals.

# The Problem

NebulaStream preallocates a pool of fixed-size `TupleBuffer`s (the operator buffer size — configurable;
our runs use 4 KiB) and hands them out through a lock-free MPMC queue with reference-counted recycling and
no per-buffer heap allocation. Any request for a *different* size — a `VARSIZED` payload, a hash-map page,
a paged-vector segment — cannot come from that pool and falls to the **unpooled** path
(`UnpooledChunksManager`): a per-thread allocator that heap-allocates a control block per buffer,
suballocates from a per-thread chunk behind a lock, and frees a chunk only once its last buffer is released.
Throughout, a *variable-sized request* means asking the pool for a buffer whose size differs from the
operator buffer size — to hold a `VARSIZED` value, an operator-state page, or any other payload — not the
`VARSIZED` column type specifically.

**P1 — the unpooled path is slower and fragments, and variable-sized data always takes it.**
A microbenchmark (2 M allocate/free pairs per thread) puts the unpooled path at 596 ns/op single-threaded
versus 186 ns for the pool (3.2×), narrowing to 1.6× at eight threads (§PoC). It also fragments: chunk
size is a rolling average of the last 100 requests and a chunk is pinned until its last buffer frees. For a
`VARSIZED` workload this tax is paid on *every* payload, and none of that memory is pooled or bounded by
the pool's accounting.

**P2 — the unpooled path is unbounded (showstopper, addressed elsewhere).**
It allocates from anonymous memory with no global cap, so large group-by/join state can exhaust memory and
the OS OOM-kills the worker. This is the priority the reviewers agree on. Bounding the unpooled byte counter
and terminating a query when the bounded budget is genuinely reached are prerequisites for retiring the
unpooled path; both are tracked as their own work (see Non-Goals) and are out of scope here.

**Correction on the network path.** An earlier draft claimed the wire format fixes the buffer size and
mis-deserializes a right-sized buffer. That was wrong: the wire carries sizes (Rust slices and vectors
store their length) and does not transmit unused bytes. The only network-side issue is *which allocator the
receive path uses*: a variable-sized child buffer arriving on a pipeline boundary is served from the
unpooled path today. §Proposed Solution routes it to a fitting size class instead — an allocator choice,
not a wire-format change.

# Goals

- **G1** — Serve a variable-sized buffer request from the pool at pool latency, so most such requests no
  longer hit the unpooled path (P1).
- **G2** — Keep the existing hot path: lock-free, reference-counted recycling, no per-buffer heap control
  block. The scheme must reuse it, not replace it. (Constrains G1.)
- **G3** — Bound the *extra* memory the scheme reserves. A workload that uses few sizes must not pay for
  many. Reserved capacity and resident memory are separate quantities and both must be controlled.
- **G4** — Ship incrementally and backward-compatibly: with the feature off, the engine behaves exactly as
  today; each piece is independently reviewable and testable.

# Non-Goals

- **NG1** — Bounding the unpooled path / OOM query termination. The P2 showstopper; #1701/#1702 (done) and
  #1700 (open).
- **NG2** — Disk spilling of operator state. As the review notes, spilling should start from a **pin/unpin
  API plus accounting** exposed to buffer users (so we can measure pinned vs. floating memory even before a
  mechanism exists), not from the buffer-manager internals. Separate design; #1699.
- **NG3** — Adaptive per-source inflight provisioning (#1713). Dropped: we have no measurement showing the
  static cap is a problem, and source buffers are released promptly under normal pressure. If evidence
  appears, it is its own design.
- **NG4** — Columnar runtime capacity, and any cross-node / cluster-wide budget.
- **NG5** — Changing the reference-counting or ownership model.

# Alternatives

The decision is narrow: how to serve a variable-sized request while keeping the pooled hot path. Three
options; the review argued for A2 and A3, so we weigh them explicitly.

**A1 — Segregated power-of-two size classes (proposed).** One pool per class, each an instance of the
existing lock-free machinery; round a request up to the smallest fitting class. *For:* reuses the hot path
verbatim (G2); a request costs a scan over the ~14 classes to pick the smallest fitting one, then the same
O(1) lock-free pop the fixed pool uses (the scan adds no measurable latency, §PoC); variable sizes become
pooled and bounded. *Against:* there are N classes to provision, most unused on any one workload, so the
provisioning policy carries real weight (§PoC quantifies this — with eager provisioning it is a footgun).

**A2 — One fixed size, composed (the DuckDB route).** Keep a single buffer size and represent anything
larger as a chain/tree of fixed buffers. *For:* no size-class provisioning, no internal fragmentation, one
hot path, nothing new to reason about in the allocator. *Against:* every consumer that needs a *contiguous*
larger region — a `VARSIZED` value, a hash-map page, a buffer serialized to the network — must handle
composition and non-contiguity. That pushes chunking logic into operators, code generation, and the wire,
and it does not help where contiguity is actually required. The complexity is moved, not removed; it is a
real and defensible choice, but it is not free.

**A3 — Virtual-memory over-allocation with a resident budget (vmcache/Umbra-lite).** Reserve a large
virtual region, keep a tight budget of *resident* pages, fault in on access. *For:* one unified budget, no
fragmentation, and a natural path to spilling later. *Against:* heavier machinery (fault handling; and
**pointer swizzling the moment ownership becomes DAG-like** rather than tree-like — needed once a buffer can
refer to another, possibly-spilled buffer through a central table of swizzled pointers). NES's ownership is
tree-like today (parent→child child-buffers), so swizzling is not yet forced; if we move to DAG ownership
we cannot avoid it.

**Measured comparison.** All three are implemented as an allocator microbenchmark
(`nes-memory/benchmarks/BufferManagerBenchmark.cpp`) and run on `sr630-wn-a-11` (Benchmark build, 1M
`alloc + touch-every-byte + free` per thread). Two request sizes bracket the design: 1 KiB (fits one
class/block) and 48 KiB (spans many). Latency is ns/op at 8 threads (lower is better); memory is physical
bytes reserved per allocation ÷ bytes requested.

| approach | 1 KiB ns/op | 48 KiB ns/op | mem 1 KiB | mem 48 KiB |
|---|--:|--:|--:|--:|
| **A1** size classes | 701 | **1741** | **1.00×** | 1.33× |
| **A2** compose-fixed | 676 | 9622 | 4.00× | 1.00× |
| **A3** vmcache (reuse) | 152 | 1724 | 4.00× | 1.00× |
| **A3** vmcache (reclaim) | 1819 | 5771 | 4.00× | 1.00× |
| unpooled (baseline) | 1157 | 1143 | ~1× | ~1× |

The numbers sharpen the trade-off the prose describes. **A2** carries *both* 4× internal waste on a small
buffer *and* 5.5× the latency on a large one, because a 48 KiB request becomes twelve 4 KiB block-pops —
the composition cost is real and lands on the hot path. **A3** splits on its own budget knob: in *reuse*
mode (freed slices kept resident) it matches A1's latency, but that discards the resident-budget benefit
that motivates it; the mode that actually bounds resident memory (`madvise(DONTNEED)` on every free) puts a
syscall on the allocation path and costs 3–10×. **A1** pays only a bounded ≤1.33× round-up and a single
O(1) pop, and is the only pooled path with no per-op syscall. This is an allocator microbenchmark, not the
integrated engine: at 48 KiB a memory-bandwidth-bound `memset` dominates every pooled path (so unpooled
looks competitive there), and A1's single-class queue shows contention under the zero-work alloc/free loop
that real per-buffer work would mask. It measures the intrinsic allocation cost, not end-to-end throughput.

We propose **A1 now** because it is the smallest step that satisfies G1 while reusing the hot path (G2),
and because it does not commit us to composition-everywhere (A2) or fault-handling and swizzling (A3)
before we need them. We do not claim A1 dominates: A2 is the right call if we accept composition across
consumers, and **A3 is the better long-term target if and when we design spilling** (NG2) and adopt a
resident-page budget. Recommendation: land A1, and re-open the A1-vs-A3 question as part of the spilling
design rather than pre-deciding it here.

# Proposed Solution

Generalize the single pool into segregated power-of-two classes. Each class is a `FixedSizeClassPool` — the
engine's existing MPMC queue plus a deque of stable-address segments — so the per-class hot path is
byte-for-byte today's pooled path (G2). `getBuffer(size)` computes the smallest fitting class
(`classIndexForSize`, a linear scan over the classes), pops from that class's queue, and **promotes** to a
larger pooled class if the best-fit is momentarily empty; only a request above the maximum class reaches
the unpooled tail (which #1701/#1702 already bound). With no size-class configuration there is exactly one
class — the operator buffer size — and behaviour is unchanged (G4).

**Which classes.** Powers of two from a configurable minimum to a maximum (our runs: 256 B … 1 MiB).
Sub-page classes are pointless for large payloads but useful for small emit outputs (a three-field result
belongs in a 256 B buffer, not a 4 KiB one), so we expose `min`/`max` and default the minimum to 256 B.

**Provisioning is the G3 knob** (`BufferProvisioningPolicy`): `LazyElastic` (default) reserves a small
floor per class and faults in regions on demand; `EagerPerClass` reserves every class fully; `TotalBudgetSplit`
divides a fixed total. §PoC shows why `LazyElastic` is the default and `EagerPerClass` is a footgun.

**Emit — one policy.** Size the output buffer to the input record count (an upper bound on output), capped
at the operator buffer size (`InputSized`, #1711). We deliberately do **not** ship several emit policies;
per the review, multiple policies make behaviour hard to reason about. If evidence later shows a
quantile-based size beats the input-count upper bound (trading occasional chunking for space), we add it
then. The complementary case the review raised — a projection that *grows* the schema so the output no
longer fits — is the same mechanism from the other side: the emit buffer is sized to the actual output
schema rather than assumed equal to the input buffer.

**Network — allocator only.** Serve an arriving variable-sized child from `getBuffer(child.size())` (a
fitting class) instead of the unpooled path. The wire is unchanged.

# Proof of Concept (reproducible)

All numbers: node `sr630-wn-a-11` (2×32 cores, 503 GiB RAM), `Benchmark` build (`-O3 -DNDEBUG`, no
logging/asserts). Reproduction commands are in the appendix; raw logs are archived with them. **These are
measured runs, not estimates** — anyone with cluster access can rerun each line.

**Microbench — does the size-class indirection cost anything?** The benchmark does 2 M `getBuffer`/release
pairs per thread. `getBuffer(size)` adds, over the fixed pool, only `classIndexForSize` (a linear scan over
the ~14 classes) before the same lock-free pop.

| threads | FixedPooled | SizeClassPooled | Unpooled |
|--:|--:|--:|--:|
| 1 | 186 ns | **173 ns** | 596 ns |
| 2 | 761 | 724 | 1059 |
| 4 | 774 | 761 | 1130 |
| 8 | 706 | 691 | 1142 |

The class index adds **no measurable latency** — SizeClassPooled tracks (in fact slightly beats, within
noise) FixedPooled across 1–8 threads — while both are 1.6–3.2× faster than the unpooled path. So the cost
of size classes is not the lookup; the win is keeping variable sizes off the unpooled path.

**Placement — does variable-sized data actually pool?** On the variable-sized YSB workload, classes *off*
sends the 256 KiB/512 KiB payloads to the unpooled path (invisible to pool accounting); classes *on* serves
them from the pooled 256 KiB and 512 KiB classes. Results are byte-identical either way (switching an
allocation from unpooled to pooled is not expected to change correctness — it is a placement change).

**Memory efficiency — how much is unused classes?** (The review's direct question.) On that workload **3 of
14 classes are used** and peak *live* pooled memory is ~1 MB. *Reserved* capacity depends entirely on the
policy:

| provisioning | reserved capacity | of which default 4 KiB pool | peak live | YSB-50M wall |
|---|--:|--:|--:|--:|
| `EagerPerClass` | **1134 MB** | 128 MB | ~1 MB | 30.9 s |
| `TotalBudgetSplit` | 311 MB | 128 MB | ~1 MB | 30.4 s |
| `LazyElastic` (default) | 176 MB | 128 MB | ~1 MB | 30.4 s |

All three pass identical results and are throughput-equivalent (30.4–30.9 s, within the fixed-pool
baseline's ~4.8 % run-to-run spread): provisioning is a memory knob, not a speed knob. Only 3 of the 14
classes are used on this workload.

The 128 MB default-class pool is the *same* the fixed-pool engine reserves today. Above it, `EagerPerClass`
reserves ~1 GB of mostly-unused classes — unacceptable; `LazyElastic` reserves ~48 MB of auxiliary-class
floors that are faulted lazily, so *resident* memory stays ~1 MB regardless of the class count. This is the
real cost of A1 and the reason provisioning is a first-class knob: choose `LazyElastic`, and resident
memory tracks the working set; choose `EagerPerClass`, and you pay for classes you never touch.

**Throughput — is there a regression?** On a 50 M-tuple YSB aggregation, enabling size classes changes
median completion time by −2.3 % (31.1 s vs 31.8 s), within the baseline's own 4.8 % run-to-run spread. We
do **not** claim a throughput win on fixed-size pipelines — there is none to claim, and none expected; the
value is memory placement (variable sizes pooled and bounded) at no throughput cost, not faster queries.

# Open Questions

- **Re-evaluate A3 with the spilling design** (NG2): if we adopt a resident-page budget, a VM-over-allocation
  manager may subsume size classes.
- **Auxiliary-class floor.** `LazyElastic` reserves ~48 MB of floors here; can the floor be zero (fault the
  first buffer of a class on first use) without a latency cliff?
- **Interception point for the bounded-budget failure** (#1700): the review notes the arbiter today only
  sits in `PipelineExecutionContext::allocateBuffer`, which not everyone uses; a `BufferManager` wrapper is
  cleaner — pending a virtual-call cost check.

# Appendix — reproduction

On the benchmark node (`sr630-wn-a-11`, 2×32 cores, 503 GiB RAM; tree at `/local-ssd/zeuchste/nes-mem`,
`Benchmark` build; run inside `nix develop . --command`):

```bash
S=cmake-build-release/nes-systests/systest/systest
MB=cmake-build-release/nes-memory/benchmarks/buffer-manager-benchmark
T=nes-systests/benchmark_small/YahooStreamingBenchmark_with_varsized.test

# microbench
$MB

# placement (classes off vs on) + per-class provisioned/used
NES_BM_STATS=1 $S -t $T                                             # classes off
NES_BM_STATS=1 $S -t $T -- --worker.enable_buffer_size_classes=true # classes on
#   read BM_CLASS lines: "size=<bytes> allocs=<served> buffers=<provisioned>"

# memory efficiency by provisioning policy
for P in LazyElastic EagerPerClass TotalBudgetSplit; do
  NES_BM_STATS=1 $S -t $T -- --worker.enable_buffer_size_classes=true \
    --worker.buffer_size_class_provisioning=$P
done

# throughput (YSB-50M): baseline vs size classes
$S -b --show-query-performance --data <TESTDATA> -t nes-systests/benchmark/YSB50M_tput.test -- \
  --worker.query_engine.number_of_worker_threads=8 [--worker.enable_buffer_size_classes=true]
```
