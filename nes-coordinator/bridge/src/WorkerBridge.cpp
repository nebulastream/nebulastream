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
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>
#include <coordinator/lib.h>
#include <rfl/json/read.hpp>

namespace NES
{

WorkerBridge::~WorkerBridge() = default;

BridgeResult make_result(const Exception& ex)
{
    return BridgeResult{
        static_cast<uint16_t>(ex.code()),
        rust::String(std::string(ex.what())),
        rust::String(ex.trace().to_string())
    };
}

BridgeResult ok()
{
    return BridgeResult{0, rust::String(), rust::String()};
}

uint64_t to_ms(const std::optional<std::chrono::system_clock::time_point>& opt)
{
    if (!opt)
        return 0;
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(opt->time_since_epoch()).count());
}

std::unique_ptr<WorkerBridge> start_worker(const rust::Str configJson)
{
    SingleNodeWorkerConfiguration config;
    if (!configJson.empty())
    {
        if (auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(std::string{configJson}))
        {
            config.overwriteConfigWithCommandLineInput(*parsed);
        }
    }
    auto bridge = std::make_unique<WorkerBridge>();
    bridge->worker = std::make_unique<SingleNodeWorker>(config);
    return bridge;
}

BridgeResult register_query(WorkerBridge& bridge, rust::Slice<const uint8_t> serializedFragment)
{
    SerializableQueryPlan proto;
    proto.ParseFromArray(serializedFragment.data(), static_cast<int>(serializedFragment.size()));
    auto plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
    if (auto result = bridge.worker->registerQuery(std::move(plan)); !result)
    {
        return make_result(result.error());
    }
    return ok();
}

BridgeResult start_query(WorkerBridge& bridge, int64_t id)
{
    if (auto result = bridge.worker->startQuery(QueryId{id}); !result)
    {
        return make_result(result.error());
    }
    return ok();
}

BridgeResult stop_query(WorkerBridge& bridge, int64_t id, uint8_t mode)
{
    if (auto result = bridge.worker->stopQuery(QueryId{id}, static_cast<QueryTerminationType>(mode)); !result)
    {
        return make_result(result.error());
    }
    return ok();
}

BridgeQueryStatus query_status(WorkerBridge& bridge, int64_t id)
{
    auto result = bridge.worker->getQueryStatus(QueryId{id});
    if (!result)
    {
        return BridgeQueryStatus{make_result(result.error()), 0, 0, 0, ok()};
    }
    const auto& snapshot = *result;
    auto query_error = snapshot.metrics.error
        ? make_result(*snapshot.metrics.error)
        : ok();
    return BridgeQueryStatus{
        ok(),
        static_cast<int32_t>(snapshot.state),
        to_ms(snapshot.metrics.start),
        to_ms(snapshot.metrics.stop),
        std::move(query_error)
    };
}

}

void enable_memcom();

namespace NES
{
void call_enable_memcom() { ::enable_memcom(); }
}
