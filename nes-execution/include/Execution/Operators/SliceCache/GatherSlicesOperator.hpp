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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>


namespace NES::Runtime::Execution::Operators
{

class GatherSlicesOperator : public ExecutableOperator
{
public:
    GatherSlicesOperator(
        uint64_t operatorHandlerIndex, std::unique_ptr<TimeFunction> timeFunction, uint64_t windowSize, uint64_t windowSlide);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    nautilus::val<Timestamp::Underlying> getSliceStart(const nautilus::val<Timestamp>& ts) const;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    uint64_t operatorHandlerIndex;
    std::unique_ptr<TimeFunction> timeFunction;
    uint64_t windowSize;
    uint64_t windowSlide;
};

}
