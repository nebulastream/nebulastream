#pragma once

#include <cstddef>
#include <WorkerLocalSingleton.hpp>

namespace NES
{

/// Worker-scoped singleton holding the Tokio IO runtime used by Rust async sources/sinks.
/// Constructed by SingleNodeWorker before worker threads are spawned, automatically
/// propagated to child threads via WorkerLocalSingleton/Thread hooks. Access via
/// IORuntime::instance().
class IORuntime : public WorkerLocalSingleton<IORuntime>
{
    size_t runtimeIdx;

public:
    IORuntime();
    ~IORuntime();

    [[nodiscard]] size_t id() const { return runtimeIdx; }
};

} // namespace NES
