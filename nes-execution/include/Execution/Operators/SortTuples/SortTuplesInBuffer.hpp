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

namespace NES::Runtime::Execution::Operators
{

class SortTuplesInBuffer : public Operator
{
public:
    SortTuplesInBuffer(
        std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProviderInput,
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections,
        Nautilus::Record::RecordFieldIdentifier sortField);
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProviderInput;
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
    Nautilus::Record::RecordFieldIdentifier sortField;
};

}
