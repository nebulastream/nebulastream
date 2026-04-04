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

void register_query(WorkerBridge& bridge, rust::Slice<const uint8_t> serializedFragment)
{
    SerializableQueryPlan proto;
    proto.ParseFromArray(serializedFragment.data(), static_cast<int>(serializedFragment.size()));
    auto plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
    if (const auto result = bridge.worker->registerQuery(std::move(plan)); !result)
    {
        throw std::runtime_error(result.error().what());
    }
}

void start_query(WorkerBridge& bridge, int64_t id)
{
    if (const auto result = bridge.worker->startQuery(QueryId{id}); !result)
    {
        throw std::runtime_error(result.error().what());
    }
}

void stop_query(WorkerBridge& bridge, int64_t id, uint8_t mode)
{
    if (const auto result = bridge.worker->stopQuery(QueryId{id}, static_cast<QueryTerminationType>(mode)); !result)
    {
        throw std::runtime_error(result.error().what());
    }
}

int32_t query_status(WorkerBridge& bridge, int64_t id)
{
    const auto result = bridge.worker->getQueryStatus(QueryId{id});
    if (!result)
    {
        throw std::runtime_error(result.error().what());
    }
    return static_cast<int32_t>(result->state);
}

}

void enable_memcom();

namespace NES
{
void call_enable_memcom() { ::enable_memcom(); }
}
