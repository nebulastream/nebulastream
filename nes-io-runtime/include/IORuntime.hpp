#pragma once

#include <cstddef>
#include <nes-io-runtime-bindings/lib.h>
#include <nlohmann/detail/meta/std_fs.hpp>
#include <nlohmann/json_fwd.hpp>
#include <rust/cxx.h>
#include <WorkerLocalSingleton.hpp>

extern "C" IORuntimeHandle* current_io_runtime_internal();
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
    friend IORuntimeHandle* ::current_io_runtime_internal();
public:
    IORuntime();
    ~IORuntime();

    void attachConfig(std::string_view serviceName, const nlohmann::json& config);
};

} // namespace NES
