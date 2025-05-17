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

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Streaming/Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Streaming/WindowBuildPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{

StreamJoinBuildPhysicalOperator::StreamJoinBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerIndex,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : WindowBuildPhysicalOperator(operatorHandlerIndex, std::move(timeFunction))
    , joinBuildSide(joinBuildSide)
    , memoryProvider(std::move(std::move(memoryProvider)))
{
}
}
