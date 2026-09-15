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

#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <nes-coordinator-bridge/worker.h>
#include <rust/cxx.h>
#include <BridgeConfig.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <SerializableQueryPlan.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SingleNodeWorkerRPCService.pb.h>
#include <Version.hpp>

/// Makes the workers of this process exchange data through memory (memcom) instead of over the network.
/// The function is exported by the Rust network crate.
/// It is called from C++, because the Rust code of this bridge would call a different copy of that crate than the one the worker uses.
extern void enable_memcom();

namespace NES::Bridge
{
namespace
{
SingleNodeWorkerConfiguration workerConfigFromJson(const rust::Str configJson)
{
    auto config = resolveConfiguration<SingleNodeWorkerConfiguration>(literalsFromJson(std::string{configJson}));
    if (!config.has_value())
    {
        throw InvalidConfigParameter("{}", config.error());
    }
    return std::move(*config);
}

/// The state is passed to Rust as a number, and Rust reads it with the numbering of the proto enum.
static_assert(static_cast<int>(QueryStatus::Registered) == static_cast<int>(::QueryState::Registered));
static_assert(static_cast<int>(QueryStatus::Started) == static_cast<int>(::QueryState::Started));
static_assert(static_cast<int>(QueryStatus::Running) == static_cast<int>(::QueryState::Running));
static_assert(static_cast<int>(QueryStatus::Stopped) == static_cast<int>(::QueryState::Stopped));
static_assert(static_cast<int>(QueryStatus::Failed) == static_cast<int>(::QueryState::Failed));

/// A cxx struct cannot hold an optional, so a time that is not set is passed as 0.
uint64_t millisecondsOrZero(const std::optional<std::chrono::system_clock::time_point>& time)
{
    return time
        .transform(
            [](const std::chrono::system_clock::time_point& point)
            { return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(point.time_since_epoch()).count()); })
        .value_or(0);
}
}

WorkerBridge::WorkerBridge(const rust::Str configJson)
{
    const auto config = workerConfigFromJson(configJson);
    /// All workers of this process share one network layer, so memcom is switched on once, before the first worker starts.
    static std::once_flag memcom;
    std::call_once(memcom, ::enable_memcom);
    worker = std::make_unique<SingleNodeWorker>(config);
}

WorkerBridge::~WorkerBridge() = default;

std::unique_ptr<WorkerBridge> start_worker(const rust::Str configJson)
{
    return std::make_unique<WorkerBridge>(configJson);
}

BridgeError validate_worker_config(const rust::Str configJson)
{
    return catchAsBridgeError([&] { workerConfigFromJson(configJson); });
}

rust::String worker_version()
{
    return rust::String{versionInfo(SingleNodeWorkerBinaryName).getRawValue()};
}

BridgeError start_query_fragment(WorkerBridge& bridge, const rust::Slice<const uint8_t> serializedFragment)
{
    return catchAsBridgeError(
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

BridgeError stop_query_fragment(WorkerBridge& bridge, const int64_t id)
{
    return catchAsBridgeError([&] { return bridge.worker->stopQuery(QueryId{id}); });
}

QueryFragmentStatus get_query_fragment_status(WorkerBridge& bridge, const int64_t id)
{
    return catchAsBridgeError(
        [&]
        {
            return bridge.worker->getQueryStatus(QueryId{id})
                .transform(
                    [](const LocalQueryStatusSnapshot& snapshot)
                    {
                        return QueryFragmentStatus{
                            .error = {},
                            .found = true,
                            .state = static_cast<int32_t>(snapshot.state),
                            .start_ms = millisecondsOrZero(snapshot.metrics.start),
                            .stop_ms = millisecondsOrZero(snapshot.metrics.stop),
                            .query_error = snapshot.metrics.error.transform(toBridgeError).value_or(BridgeError{})};
                    })
                .or_else(
                    [](const Exception& error) -> std::expected<QueryFragmentStatus, Exception>
                    {
                        /// The coordinator polls this call, and a worker that does not know the query is a normal result.
                        /// It is reported as a field, so that no stack trace is rendered on every poll.
                        if (error.code() == ErrorCode::QueryNotFound)
                        {
                            return QueryFragmentStatus{
                                .error = {}, .found = false, .state = 0, .start_ms = 0, .stop_ms = 0, .query_error = {}};
                        }
                        return std::unexpected{error};
                    });
        });
}

}
