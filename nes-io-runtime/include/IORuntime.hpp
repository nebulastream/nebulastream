#pragma once

#include <cstddef>
#include <rust/cxx.h>
#include <nes-io-runtime-bindings/lib.h>
#include <WorkerLocalSingleton.hpp>

namespace NES
{

/// Worker-scoped singleton holding the Tokio IO runtime used by Rust async sources/sinks.
///
/// Constructed by SingleNodeWorker before worker threads are spawned, automatically
/// propagated to child threads via WorkerLocalSingleton/Thread hooks. Access via
/// `IORuntime::instance()`.
///
/// The Rust runtime + per-runtime registries live in a `rust::Box<IORuntimeHandle>`
/// owned by this class. The Box's destructor joins the Tokio worker threads, so
/// every spawned task is finished before the runtime is freed.
class IORuntime : public WorkerLocalSingleton<IORuntime>
{
    rust::Box<IORuntimeHandle> handle;

public:
    IORuntime();
    ~IORuntime();

    /// Reference to the Rust opaque handle. Used for FFI calls that take a
    /// `&IORuntimeHandle` (e.g. `attach_config`).
    [[nodiscard]] const IORuntimeHandle& rustHandle() const { return *handle; }

    /// Address of the underlying Rust handle, for FFI bridges that cannot
    /// declare the cxx-opaque type (cxx 1.0 limitation: opaque Rust types
    /// cannot be shared across crates). The bridge casts this back to the
    /// matching Rust type internally — see e.g. `create_handle` in
    /// `nes_sink_runtime_bindings`.
    [[nodiscard]] std::size_t rustHandleAddress() const { return reinterpret_cast<std::size_t>(&*handle); }
};

} // namespace NES
