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
#include <optional>
#include <utility>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <coordinator/lib.h>
#include <cpptrace/from_current.hpp>
#include <rfl/json/read.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Version.hpp>

namespace NES
{

WorkerBridge::~WorkerBridge() = default;

namespace
{
/// Folds both failure channels of a worker call into one error: an Exception the call returns, and one thrown before
/// the worker is reached. The generated cxx shims are `noexcept`, so an escaping exception would terminate.
template <typename Call>
BridgeError guard(Call&& call)
{
    CPPTRACE_TRY
    {
        if (auto result = std::forward<Call>(call)(); !result)
        {
            return make_error(result.error());
        }
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return make_error(wrapExternalException());
    }
    std::unreachable();
}

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
            return bridge.worker->startQuery(QueryPlanSerializationUtil::deserializeQueryPlan(proto));
        });
}

BridgeError stop_query(WorkerBridge& bridge, int64_t id)
{
    return guard([&] { return bridge.worker->stopQuery(QueryId{id}); });
}

BridgeQueryStatus query_status(WorkerBridge& bridge, int64_t id)
{
    /// The remaining fields stay zeroed unless the read succeeded.
    BridgeQueryStatus status{};
    status.error = guard(
        [&]() -> std::expected<void, Exception>
        {
            auto snapshot = bridge.worker->getQueryStatus(QueryId{id});
            if (!snapshot)
            {
                return std::unexpected{snapshot.error()};
            }
            status.state = static_cast<int32_t>(snapshot->state);
            status.start_ms = to_ms(snapshot->metrics.start);
            status.stop_ms = to_ms(snapshot->metrics.stop);
            if (snapshot->metrics.error)
            {
                status.query_error = make_error(*snapshot->metrics.error);
            }
            return {};
        });
    return status;
}

}

void enable_memcom();

namespace NES
{
void call_enable_memcom()
{
    ::enable_memcom();
}
}
