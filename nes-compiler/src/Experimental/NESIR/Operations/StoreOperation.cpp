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

#include <Experimental/NESIR/Operations/StoreOperation.hpp>

namespace NES {

StoreOperation::StoreOperation(void* outputBufferPtr, std::vector<OperationPtr> valuesToStore, std::vector<uint64_t> outputBufferIndexes)
    : Operation(OperationType::StoreOp), outputBufferPtr(outputBufferPtr), valuesToStore(valuesToStore), outputBufferIndexes(outputBufferIndexes) {}

void* StoreOperation::getOutputBuffer() { return outputBufferPtr; }

std::vector<OperationPtr> StoreOperation::getValuesToStore() { return valuesToStore; }

std::vector<uint64_t> StoreOperation::getOutputBufferIndexes() { return outputBufferIndexes; }

bool StoreOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::StoreOp; }
}// namespace NES
