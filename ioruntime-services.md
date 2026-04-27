# IORuntime Services — Idea

A per-`IORuntime` (per-embedded-worker) registry where Rust sources/sinks
register and look up shared services (network sender/receiver, future MQTT
broker, ...) by type. Services come up on first use and shut down when the
last user goes away. No cross-talk between workers in the same process.

## Goal

- **Worker-scoped sharing.** Sources/sinks running on the same IORuntime share
  the same service instance; sources/sinks running on a different worker's
  IORuntime get a different one.
- **On-demand lifecycle.** A service materializes when the first sink/source
  that needs it calls `get_or_init`. When the last `Arc<T>` drops, the service
  shuts down. Re-creates on next access.
- **No global hashmap keyed by address.** Replaces today's
  `nes_network::registry::SERVICES: LazyLock<Mutex<HashMap<Addr, ...>>>`.
- **No per-task scoping.** We do *not* want `tokio::task_local!` semantics —
  sinks/sources on the same worker must share, not be siloed per task.

## Discovery — finding "my IORuntime" from a Rust task

Rust thread-local set inside Tokio's `on_thread_start`. Every worker thread of
this IORuntime stores `Weak<IORuntime>`; `current_io_runtime()` upgrades it.
Tasks moving between worker threads of the same runtime always land on a
worker thread that has the same Weak set. This is the same pattern Tokio's own
`Handle::current()` uses internally.

```rust
thread_local! {
    static CURRENT: Cell<Option<Weak<IORuntime>>> = const { Cell::new(None) };
}

pub fn current_io_runtime() -> Arc<IORuntime> {
    CURRENT.with(|c| c.get().as_ref().and_then(Weak::upgrade))
        .expect("not on an IORuntime worker thread")
}
```

`Weak`, not `Arc`, so the Drop of `IORuntime` is not blocked by stale
thread-locals (Tokio joins worker threads as part of dropping its `Runtime`,
but pinning an `Arc<IORuntime>` per thread would create a cycle).

## Service registry — the core data structure

Type-id-keyed, `Weak<dyn Any + Send + Sync>` in the map, `Arc<T>` handed out.

```rust
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

struct Slot {
    cell: OnceLock<Weak<dyn Any + Send + Sync>>,
}

pub struct ServiceRegistry {
    slots: Mutex<HashMap<TypeId, Arc<Slot>>>,
}

impl ServiceRegistry {
    pub fn get_or_init<T, F>(&self, init: F) -> Arc<T>
    where
        T: Any + Send + Sync + 'static,
        F: FnOnce() -> Arc<T>,
    {
        // Registry mutex held only for the lookup.
        let slot = self.slots.lock().unwrap()
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Arc::new(Slot { cell: OnceLock::new() }))
            .clone();

        // Fast path: live entry.
        if let Some(weak) = slot.cell.get() {
            if let Some(strong) = weak.upgrade() {
                return strong.downcast::<T>().unwrap();
            }
            // Stale slot — replace with a fresh OnceLock.
            self.replace_stale_slot::<T>();
            return self.get_or_init(init);
        }

        // Slow path: init runs without holding the registry mutex.
        // Concurrent same-type callers serialize on this slot's OnceLock only.
        let strong = init();
        let as_any: Arc<dyn Any + Send + Sync> = strong.clone();
        let _ = slot.cell.set(Arc::downgrade(&as_any));
        strong
    }
}
```

Plugin-side usage:

```rust
async fn start(&mut self) -> Result<()> {
    let svc = current_io_runtime().services()
        .get_or_init::<NetworkSender, _>(|| Arc::new(NetworkSender::start(...)));
    let channel = svc.register_channel(self.channel_id.clone(), ...).await?;
    ...
}
```

## Why these particular choices

### Why `Weak<T>` in the map, not `Arc<T>`

If the registry held `Arc<T>`, the strong count would never hit 0 and Drop
would never fire. Holding `Weak<T>` gives us automatic auto-shutdown when the
last consumer drops their `Arc`.

Stale entries (Weak whose strong count is 0) accumulate. Cleanup is
opportunistic — on each insert we can `retain(|_, s| s.cell.get().and_then(|w|
w.strong_count()).unwrap_or(0) > 0)`. For a handful of services per worker
this is fine. If service-type churn becomes high, switch to `weak-table`.

### Why sync `Mutex`, not `tokio::sync::Mutex`

The registry critical section is HashMap lookup + `Weak::upgrade` — a few
microseconds. Tokio's official guidance is "sync mutex for short-lived
critical sections, async only when holding across `.await`". The `init()`
closure runs *outside* the registry mutex (held only behind the per-slot
`OnceLock`), so even slow constructors don't block the registry.

If a service ever needs `async fn init`, swap `OnceLock` →
`tokio::sync::OnceCell`. Don't pre-bake — every existing service in this
codebase is `start(runtime, ...) -> Arc<Self>` (sync; the IO work is spawned
internally and runs lazily).

### Why TypeId keying (single instance per type)

Simplest viable design. Constraint: "one network sender per worker," "one
MQTT broker per worker." If we ever need two distinct instances of the same
type (e.g., two MQTT clusters), the escape hatch is to newtype them
(`struct ClusterAMqtt(MqttBroker)`) or switch to `(TypeId, &'static str)`
keys. Don't build the multi-instance machinery until a service needs it.

## Async shutdown — `Drop` is sync

Three options, in increasing order of cleanliness:

1. **Sync `Drop` sends a stop signal.** Internal `JoinHandle`s abort via their
   own Drop. Fast, slightly rough — tasks may still be running briefly after
   the `Arc<T>` is dropped.
2. **`Drop` spawns a detached cleanup task** on the IORuntime. Cleaner, but
   if the IORuntime itself is concurrently shutting down, the spawn fails
   silently.
3. **Explicit async shutdown driven by the registry** when the strong count
   transitions 1 → 0 (only the registry holds the last ref). More machinery,
   stronger guarantees.

Start with (1). Existing `NetworkService::shutdown(self)` already takes
`self` by move and is essentially "send close, drop join handles" — moving
that into `Drop` is small.

## Race: re-init while `T::drop` is still running

Window: between "last `Arc<T>` drops, T::drop begins" and "next caller's
`Weak::upgrade` returns None, init runs." A new T can come up before the old
T's Drop finishes. Almost always benign. Only matters for services that bind
exclusive resources (TCP listener on a fixed port without `SO_REUSEADDR`).
For our network service we already use `SO_REUSEADDR` paths or ephemeral
ports — should be a non-issue.

## What this replaces / unblocks

- Deletes `nes_network::registry::SERVICES` (the address-keyed `LazyLock<Mutex<HashMap<Addr, ...>>>`).
- Drops the `BIND` config field from `NetworkSink`/`NetworkSource` plugins —
  there's no longer an address to look up by; each plugin just asks the
  worker's registry for the shared service.
- Drops the `runtime_idx` plumbing through `create_handle` (C++ → Rust):
  inside the spawned task we already know our IORuntime via the thread-local,
  so the FFI doesn't need to pass it.
- Future MQTT broker / Kafka consumer group / etc. drop into the same shape.
  No `IORuntime` change per service — just `current_io_runtime().services().get_or_init::<MyBroker, _>(...)`.

## Open questions

- **Per-worker network listener config.** Today `init_*_service(addr, ...)`
  is called from `SingleNodeWorker` with the worker's `dataAddress`. With
  on-demand init, where does the bind address come from? The sink/source
  knows nothing about it. Two options:
  1. Make the receiver's bind address part of `IORuntime` construction
     (passed in alongside thread count). The first plugin that needs the
     receiver service uses that as `init()` input.
  2. Keep `init_receiver_service` explicit — call it from `SingleNodeWorker`
     after `IORuntime` is constructed, which inserts the
     `Arc<ReceiverService>` directly into the registry. On-demand init is
     for *new* services without C++-side config.
  Probably (2) for the network service (preserves existing config UX),
  (1) for fully Rust-internal services like a future MQTT broker.

- **Test ergonomics.** Worker tests today don't construct an IORuntime. If
  test code calls `current_io_runtime()` outside a worker thread, it panics.
  Need a test fixture that spins up a small IORuntime, or `tryInstance`-style
  fallible accessor.

- **Diagnostics.** Same bar as the C++-side `WorkerLocalSingleton::instance()`
  panic message: when `current_io_runtime()` is called from a non-IORuntime
  thread, the panic should say *which* thread and what was expected.

## Next steps

1. Add `Arc<ServiceRegistry>` field on Rust `IORuntime`, add the thread-local
   + `on_thread_start` wiring, expose `current_io_runtime()`.
2. Migrate `nes-network`'s `init_sender_service` / `init_receiver_service` to
   insert into the worker's registry instead of the global `SERVICES` map.
3. Update `NetworkSink` / `NetworkSource` plugin code to look up via
   `current_io_runtime().services().get::<...>()` and drop the `BIND` config.
4. Drop the `runtime_idx` parameter from `create_handle` in the sink/source
   FFI bridges — the spawned task discovers its IORuntime via thread-local.
5. Delete the old `nes_network::registry::SERVICES` global.
