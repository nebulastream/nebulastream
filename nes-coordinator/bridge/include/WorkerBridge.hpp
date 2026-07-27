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

#include <memory>
#include <rust/cxx.h>

namespace NES
{
class SingleNodeWorker;

struct WorkerBridge
{
    std::unique_ptr<SingleNodeWorker> worker;
    ~WorkerBridge();
};

struct BridgeResult;
struct BridgeQueryStatus;

std::unique_ptr<WorkerBridge> start_worker(rust::Str configJson);
BridgeResult register_query(WorkerBridge& bridge, rust::Slice<const uint8_t> serializedFragment);
BridgeResult start_query(WorkerBridge& bridge, int64_t id);
BridgeResult stop_query(WorkerBridge& bridge, int64_t id, uint8_t mode);
BridgeQueryStatus query_status(WorkerBridge& bridge, int64_t id);
void call_enable_memcom();
}
