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
#include <Functions/PhysicalFunction.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Windowing/WindowMetaData.hpp>

namespace NES
{
using namespace Interface::MemoryProvider;

/// Performs the second phase of the join. The tuples are joined via two nested loops. The left stream is the outer loop, and the right stream is the inner loop.
class NLJProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    NLJProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        const JoinSchema& joinSchema,
        std::shared_ptr<TupleBufferMemoryProvider> leftMemoryProvider,
        std::shared_ptr<TupleBufferMemoryProvider> rightMemoryProvider);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

protected:
    std::shared_ptr<TupleBufferMemoryProvider> leftMemoryProvider;
    std::shared_ptr<TupleBufferMemoryProvider> rightMemoryProvider;
};
}
