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

#include <cstddef>
#include <memory>
#include <utility>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PhysicalOperator.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
 * Furthermore, it supports projection pushdown to eliminate unneeded reads.
 */
class SequencePhysicalOperator : public PhysicalOperatorConcept
{
public:
    explicit SequencePhysicalOperator(OperatorHandlerId operatorHandlerIndex, ScanPhysicalOperator scan)
        : scan(std::move(scan)), operatorHandlerIndex(operatorHandlerIndex)
    {
    }

    void open(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext&, RecordBuffer&) const override { /*NOOP*/ }

    void setup(ExecutionContext& executionCtx) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<struct PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;


private:
    ScanPhysicalOperator scan;
    OperatorHandlerId operatorHandlerIndex;
};

}
