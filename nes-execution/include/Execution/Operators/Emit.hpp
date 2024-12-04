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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>

namespace NES::Runtime::Execution::Operators
{

/**
 * @brief Basic emit operator that receives records from an upstream operator and
 * writes them to a tuple buffer according to a memory layout.
 */
class Emit : public ExecutableOperator
{
public:
    explicit Emit(std::unique_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider);
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    static void emitRecordBuffer(
        ExecutionContext& ctx, RecordBuffer& recordBuffer, const nautilus::val<uint64_t>& numRecords, const nautilus::val<bool>& lastChunk);

private:
    uint64_t maxRecordsPerBuffer;
    std::unique_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
};

}
