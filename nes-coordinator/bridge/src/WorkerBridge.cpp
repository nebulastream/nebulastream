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

#include <WorkerBridge.hpp>

#include <expected>
#include <mutex>
#include <optional>
#include <utility>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <nes-coordinator-bridge/worker.h>
#include <rfl/json/read.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Version.hpp>

/// Switches the workers of this process to memcom, the transport that moves data between them through memory.
/// The switch is in the network crate's copy that is linked into the worker,
/// so it is reached through the crate's exported C++ symbol rather than from the Rust side of this bridge.
void enable_memcom();

namespace NES::Bridge
{

WorkerBridge::~WorkerBridge() = default;

namespace
{
uint64_t to_ms(const std::optional<std::chrono::system_clock::time_point>& opt)
{
    if (!opt)
        return 0;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(opt->time_since_epoch()).count());
}
}

rust::String worker_version()
{
    return rust::String{versionInfo(SingleNodeWorkerBinaryName).getRawValue()};
}

std::unique_ptr<WorkerBridge> start_worker(const rust::Str configJson)
{
    SingleNodeWorkerConfiguration config;
    if (!configJson.empty())
    {
        auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string{configJson});
        if (!parsed)
        {
            throw CannotDeserialize("Failed to deserialize worker config from JSON: {}", parsed.error().what());
        }
        config.overwriteConfigWithCommandLineInput(*parsed);
    }
    /// Every worker started here shares the process, so the switch is set once, before the first worker's network layer reads it.
    static std::once_flag memcom;
    std::call_once(memcom, ::enable_memcom);

    auto bridge = std::make_unique<WorkerBridge>();
    bridge->worker = std::make_unique<SingleNodeWorker>(config);
    return bridge;
}

BridgeError start_query(WorkerBridge& bridge, rust::Slice<const uint8_t> serializedFragment)
{
    return guard(
        [&]
        {
            SerializableQueryPlan proto;
            if (!proto.ParseFromArray(serializedFragment.data(), static_cast<int>(serializedFragment.size())))
            {
                throw CannotDeserialize("query plan protobuf is malformed ({} bytes)", serializedFragment.size());
            }
            return bridge.worker->startQuery(QueryPlanSerializationUtil::deserializeQueryPlan(proto)).transform([](const QueryId&) { });
        });
}

BridgeError stop_query(WorkerBridge& bridge, int64_t id)
{
    return guard([&] { return bridge.worker->stopQuery(QueryId{id}); });
}

BridgeQueryStatus query_status(WorkerBridge& bridge, int64_t id)
{
    return guard(
        [&]() -> std::expected<BridgeQueryStatus, Exception>
        {
            auto snapshot = bridge.worker->getQueryStatus(QueryId{id});
            if (!snapshot)
            {
                return std::unexpected{snapshot.error()};
            }
            BridgeQueryStatus status{};
            status.state = static_cast<int32_t>(snapshot->state);
            status.start_ms = to_ms(snapshot->metrics.start);
            status.stop_ms = to_ms(snapshot->metrics.stop);
            if (snapshot->metrics.error)
            {
                status.query_error = make_error(*snapshot->metrics.error);
            }
            return status;
        });
}

}
