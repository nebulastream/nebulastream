/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#pragma once

#include <cstddef>
#include <string_view>
#include <nes-io-runtime-bindings/lib.h>
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

    void attachConfig(std::string_view serviceName, std::string_view config);
};

} /// namespace NES
