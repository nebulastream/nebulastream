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
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution::Operators
{

/// Performs the second phase of the join. The tuples are joined via two nested loops.
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
    void performNLJ(
        const Interface::PagedVectorRef& outerPagedVector,
        const Interface::PagedVectorRef& innerPagedVector,
        Interface::MemoryProvider::TupleBufferMemoryProvider& outerMemoryProvider,
        Interface::MemoryProvider::TupleBufferMemoryProvider& innerMemoryProvider,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const; const

    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider;
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider;
};
}
