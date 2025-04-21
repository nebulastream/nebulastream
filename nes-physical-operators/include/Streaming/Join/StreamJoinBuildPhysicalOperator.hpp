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

#include <cstdint>
#include <memory>
#include <Abstract/PhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Streaming/WindowBuildPhysicalOperator.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{
/// This class is the first phase of the stream join. The actual implementation is not part of this class
/// It only takes care of the close() and terminate() functionality as these are universal
class StreamJoinBuildPhysicalOperator : public WindowBuildPhysicalOperator
{
public:
    StreamJoinBuildPhysicalOperator(
        std::shared_ptr<TupleBufferMemoryProvider> memoryProvider,
        OperatorHandlerId operatorHandlerIndex,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction);

protected:
    const JoinBuildSideType joinBuildSide;
    std::shared_ptr<TupleBufferMemoryProvider> memoryProvider;
};

}
