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

#include <Nautilus/IR/Operations/AddressOperation.hpp>
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <string>

namespace NES::Nautilus::IR::Operations {
AddressOperation::AddressOperation(OperationIdentifier identifier,
                                   PrimitiveStamp dataType,
                                   uint64_t getRecordWidth,
                                   uint64_t fieldOffset,
                                   std::string recordIdxName,
                                   std::string addressSourceName)
    : Operation(Operation::OperationType::AddressOp, identifier, Types::StampFactory::createAddressStamp()), dataType(dataType),
      recordWidth(getRecordWidth), fieldOffset(fieldOffset), recordIdxName(recordIdxName), addressSourceName(addressSourceName) {}

PrimitiveStamp AddressOperation::getDataType() { return dataType; }
uint64_t AddressOperation::getRecordWidthInBytes() { return recordWidth; }
uint64_t AddressOperation::getFieldOffsetInBytes() { return fieldOffset; }
std::string AddressOperation::getRecordIdxName() { return recordIdxName; }
std::string AddressOperation::getAddressSourceName() { return addressSourceName; }

std::string AddressOperation::toString() {
    return "AddressOperation_" + identifier + "(" + std::to_string(recordWidth) + ", " + std::to_string(fieldOffset) + ", "
        + recordIdxName + ", " + addressSourceName + ")";
}

bool AddressOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::AddressOp; }
}// namespace NES::Nautilus::IR::Operations