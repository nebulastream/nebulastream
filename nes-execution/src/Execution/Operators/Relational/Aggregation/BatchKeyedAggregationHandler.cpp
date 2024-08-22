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

#include <utility>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
namespace Runtime
{
class ReconfigurationMessage;
enum class QueryTerminationType : uint8_t;
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution::Operators
{

BatchKeyedAggregationHandler::BatchKeyedAggregationHandler()
{
}

Nautilus::Interface::ChainedHashMap* BatchKeyedAggregationHandler::getThreadLocalStore(WorkerThreadId workerThreadId)
{
    auto index = workerThreadId % threadLocalSliceStores.size();
    return threadLocalSliceStores[index].get();
}

void BatchKeyedAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t keySize, uint64_t valueSize)
{
    /// TODO: provide a way to indicate the number of keys from the outside.
    auto numberOfKeys = 1000;
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++)
    {
        auto allocator = std::make_unique<NesDefaultMemoryAllocator>();
        auto hashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfKeys, std::move(allocator));
        threadLocalSliceStores.emplace_back(std::move(hashMap));
    }
}

void BatchKeyedAggregationHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t)
{
    NES_DEBUG("start GlobalSlicePreAggregationHandler");
}

void BatchKeyedAggregationHandler::stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr)
{
    NES_DEBUG("shutdown GlobalSlicePreAggregationHandler: {}", queryTerminationType);
}
BatchKeyedAggregationHandler::~BatchKeyedAggregationHandler()
{
    NES_DEBUG("~GlobalSlicePreAggregationHandler");
}

void BatchKeyedAggregationHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&)
{
}

} /// namespace NES::Runtime::Execution::Operators
