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

#include "API/Schema.hpp"
#include "Common/PhysicalTypes/BasicPhysicalType.hpp"
#include "Experimental/Interpreter/FunctionCall.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Experimental/Interpreter/Operators/CustomScan.hpp>
#include <Experimental/Interpreter/Record.hpp>

#include <Experimental/Interpreter/ProxyFunctions.hpp>



namespace NES::ExecutionEngine::Experimental::Interpreter {

CustomScan::CustomScan(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout) : memoryLayout(memoryLayout) {}


void CustomScan::open(RuntimeExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // call open on all child operators
    child->open(ctx, recordBuffer);
    // iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    auto bufferAddress = recordBuffer.getBuffer();
    std::cout << "Size: " << numberOfRecords << '\n';
    std::cout << "Num records: " << numberOfRecords << '\n';
    for (Value<UInt64> i = 0ul; i <= numberOfRecords; i = i + 1ul) {
        // auto record = recordBuffer.read(memoryLayout, bufferAddress, i);
        auto rowLayout = std::dynamic_pointer_cast<Runtime::MemoryLayouts::RowLayout>(memoryLayout);
        auto tupleSize = rowLayout->getTupleSize();
        std::vector<Value<Any>> fieldValues;
        for (uint64_t j = 0; j < rowLayout->getSchema()->getSize(); j++) {
            auto fieldOffset = rowLayout->getFieldOffSets()[j];
            auto offset = tupleSize * i + fieldOffset;
            auto fieldAddress = bufferAddress + offset;
            std::cout << "Field address: " << fieldAddress << '\n';
            auto memRef = fieldAddress.as<MemRef>();
            auto value = memRef.load<Int64>();
            // FunctionCall<>("printInt64", NES::Runtime::ProxyFunctions::printInt64, value);
            FunctionCall<>("printInt64", NES::Runtime::ProxyFunctions::printInt64, i);
            // FunctionCall<>("testFunc", NES::Runtime::ProxyFunctions::printInt64, fieldAddress.as<Int64>());
            // auto hashResult = FunctionCall<>("testFunc", NES::Runtime::ProxyFunctions::printInt64, value);
            fieldValues.emplace_back(value);
            // fieldValues.emplace_back(hashResult);
        }
        auto record = Record(fieldValues);
        child->execute(ctx, record);
    }
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter