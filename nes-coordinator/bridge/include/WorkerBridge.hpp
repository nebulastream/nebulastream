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

#include <cstdint>
#include <memory>

#include <rust/cxx.h>

namespace NES
{
class SingleNodeWorker;
}

namespace NES::Bridge
{

/// One worker that runs inside the process of the coordinator.
/// The Rust side holds it as an opaque handle.
struct WorkerBridge
{
    /// Starts the worker, and throws when the configuration is invalid.
    explicit WorkerBridge(rust::Str configJson);
    /// Defined in the source file, where the worker type is complete.
    ~WorkerBridge();

    std::unique_ptr<SingleNodeWorker> worker;
};

struct BridgeError;
struct QueryFragmentStatus;

/// cxx cannot call a constructor, so Rust creates a worker through this function.
/// A failure is thrown, and Rust receives its message without the error code.
std::unique_ptr<WorkerBridge> start_worker(rust::Str configJson);

/// Checks a worker configuration without starting a worker.
BridgeError validate_worker_config(rust::Str configJson);

/// The version of the worker that this binary would start.
/// No worker is started for it, because every worker in this process is the build that this binary was linked with.
rust::String worker_version();

/// The three functions below control one query fragment on the worker.
/// A failure is returned in the error struct.
BridgeError start_query_fragment(WorkerBridge& bridge, rust::Slice<const uint8_t> serializedFragment);
BridgeError stop_query_fragment(WorkerBridge& bridge, int64_t id);
QueryFragmentStatus get_query_fragment_status(WorkerBridge& bridge, int64_t id);
}
