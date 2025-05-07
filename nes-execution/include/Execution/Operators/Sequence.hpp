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

#include <memory>
#include <vector>
#include <Execution/Operators/Operator.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

#include "Scan.hpp"

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
 * Furthermore, it supports projection pushdown to eliminate unneeded reads.
 */
class Sequence : public Operator
{
public:
    explicit Sequence(size_t operatorHandlerIndex, std::shared_ptr<Scan> scan)
        : scan(std::move(scan)), operatorHandlerIndex(operatorHandlerIndex)
    {
    }
    /**
     * @brief Constructor for the scan operator that receives a memory layout and a projection vector.
     * @param memoryProvider memory layout that describes the tuple buffer.
     * @param projections projection vector
     */
    Sequence();

    void open(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext&, RecordBuffer&) const override { /*NOOP*/ }

    void setup(ExecutionContext& executionCtx) const override;
    void terminate(ExecutionContext& executionCtx) const override;


private:
    std::shared_ptr<Scan> scan;
    size_t operatorHandlerIndex;
};

}
