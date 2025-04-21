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

#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Streaming/Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Streaming/WindowBuildPhysicalOperator.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES
{

StreamJoinBuildPhysicalOperator::StreamJoinBuildPhysicalOperator(
    std::shared_ptr<TupleBufferMemoryProvider> memoryProvider,
    const uint64_t operatorHandlerIndex,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction)
    : WindowBuildPhysicalOperator(operatorHandlerIndex, std::move(timeFunction))
    , joinBuildSide(joinBuildSide)
    , memoryProvider(std::move(memoryProvider))
{
}
}
