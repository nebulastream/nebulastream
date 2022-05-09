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

#include <Experimental/NESIR/Operations/AddressOperation.hpp>

namespace NES {
AddressOperation::AddressOperation(Operation::BasicType dataType, uint64_t getRecordWidth, uint64_t fieldOffset, bool isInputBuffer)
    : Operation(NES::Operation::AddressOp), dataType(dataType), recordWidth(getRecordWidth), fieldOffset(fieldOffset), 
    isInputBuffer(isInputBuffer) {}


Operation::BasicType AddressOperation::getDataType() { return dataType; }

uint64_t AddressOperation::getRecordWidth() { return recordWidth; }

uint64_t AddressOperation::getFieldOffset() { return fieldOffset; };

bool AddressOperation::getIsInputBuffer() { return isInputBuffer; };

bool NES::AddressOperation::classof(const NES::Operation *Op) {
    return Op->getOperationType() == OperationType::AddressOp;
}
}// namespace NES