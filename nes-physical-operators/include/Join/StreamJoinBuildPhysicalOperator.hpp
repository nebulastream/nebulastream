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
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{
/// This class is the first phase of the stream join. The actual implementation is not part of this class
class StreamJoinBuildPhysicalOperator : public WindowBuildPhysicalOperator
{
public:
    StreamJoinBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider);

    StreamJoinBuildPhysicalOperator(const StreamJoinBuildPhysicalOperator& other) = default;

    /// Updates the slices in the slice store and passes/emits slices that are ready to the second join phase (probe) for further processing
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

protected:
    const JoinBuildSideType joinBuildSide;
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
};

}
