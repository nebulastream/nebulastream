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

#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Experimental/Interpreter/Operators/Scan.hpp>
#include <Experimental/Interpreter/Record.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

Scan::Scan(const Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayout) : memoryLayout(memoryLayout) {}

void Scan::open(RuntimeExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // call open on all child operators
    child->open(ctx, recordBuffer);
    // iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    auto bufferAddress = recordBuffer.getBuffer();
    for (Value<Integer> i = 0; i <= numberOfRecords; i = i + 1) {
        auto record = recordBuffer.read(memoryLayout,bufferAddress, i);
        child->execute(ctx, record);
    }
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter