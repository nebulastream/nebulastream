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

#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>

#include <Execution/Operators/Streaming/Aggregations/ThreadLocalSliceStore.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
namespace Runtime
{
enum class QueryTerminationType : uint8_t;
namespace Execution
{
class PipelineExecutionContext;
} /// namespace Execution
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution::Operators
{

NonKeyedSliceMergingHandler::NonKeyedSliceMergingHandler()
{
}

void NonKeyedSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize)
{
    this->entrySize = entrySize;
    defaultState = std::make_unique<State>(entrySize);
}

void NonKeyedSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t)
{
    NES_DEBUG("start NonKeyedSliceMergingHandler");
}

GlobalSlicePtr NonKeyedSliceMergingHandler::createGlobalSlice(SliceMergeTask<NonKeyedSlice>* sliceMergeTask)
{
    return std::make_unique<NonKeyedSlice>(entrySize, sliceMergeTask->startSlice, sliceMergeTask->endSlice, defaultState);
}

const State* NonKeyedSliceMergingHandler::getDefaultState() const
{
    return defaultState.get();
}

NonKeyedSliceMergingHandler::~NonKeyedSliceMergingHandler()
{
    NES_DEBUG("Destruct NonKeyedSliceMergingHandler");
}

void NonKeyedSliceMergingHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr)
{
    NES_DEBUG("stop NonKeyedSliceMergingHandler");
}

} /// namespace NES::Runtime::Execution::Operators
