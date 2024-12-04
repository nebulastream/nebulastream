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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES::Runtime::Execution::Operators
{

/// Performs the second phase of the join. The tuples are joined via two nested loops. The left stream is the outer loop, and the right stream is the inner loop.
class NLJProbe final : public StreamJoinProbe
{
public:
    NLJProbe(
        uint64_t operatorHandlerIndex,
        const std::shared_ptr<Functions::Function>& joinFunction,
        const WindowMetaData& windowMetaData,
        const JoinSchema& joinSchema,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

protected:
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider;
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider;
};
}
