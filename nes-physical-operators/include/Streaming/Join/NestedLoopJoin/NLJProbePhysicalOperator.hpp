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
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Streaming/Join/StreamJoinProbePhysicalOperator.hpp>
#include <Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Execution.hpp>

namespace NES
{
using namespace Interface::MemoryProvider;

/// Performs the second phase of the join. The tuples are joined via two nested loops. The left stream is the outer loop, and the right stream is the inner loop.
class NLJProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    NLJProbePhysicalOperator(
        const uint64_t operatorHandlerIndex,
        Functions::PhysicalFunction joinFunction,
        const std::string windowStartFieldName,
        const std::string windowEndFieldName,
        const JoinSchema& joinSchema,
        std::shared_ptr<TupleBufferMemoryProvider> leftMemoryProvider,
        std::shared_ptr<TupleBufferMemoryProvider> rightMemoryProvider);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    std::optional<PhysicalOperator> getChild() const override { return child; }
    void setChild(struct PhysicalOperator child) override { this->child = child; }

private:
    std::optional<PhysicalOperator> child;
    std::shared_ptr<TupleBufferMemoryProvider> leftMemoryProvider;
    std::shared_ptr<TupleBufferMemoryProvider> rightMemoryProvider;
    static constexpr bool PIPELINE_BREAKER = true;
};
}
