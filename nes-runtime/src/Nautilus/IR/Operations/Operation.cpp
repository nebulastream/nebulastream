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

#include <Nautilus/IR/Operations/Operation.hpp>
#include <string>

namespace NES::Nautilus::IR::Operations {
Operation::Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp)
    : opType(opType), identifier(identifier), stamp(stamp) {}
Operation::Operation(OperationType opType, Types::StampPtr stamp) : opType(opType), identifier(0,0), stamp(stamp) {}
Operation::OperationType Operation::getOperationType() const { return opType; }
OperationIdentifier Operation::getIdentifier() { return identifier; }
OperationIdentifier& Operation::getIdentifierRef() { return identifier; }
const Types::StampPtr& Operation::getStamp() const { return stamp; }

void Operation::addUsage(const Operation* operation) { usages.emplace_back(operation); }

const std::vector<const Operation*>& Operation::getUsages() { return usages; }

OperationIdentifier::OperationIdentifier(uint32_t blockId, uint32_t operationId) : blockId(blockId), operationId(operationId) {}
std::ostream& operator<<(std::ostream& os, const OperationIdentifier& identifier) {
    os << identifier.blockId << "_" << identifier.operationId;
    return os;
}
bool OperationIdentifier::operator==(const OperationIdentifier& rhs) const {
    return blockId == rhs.blockId && operationId == rhs.operationId;
}
bool OperationIdentifier::operator!=(const OperationIdentifier& rhs) const { return !(rhs == *this); }

std::string OperationIdentifier::toString() const {
    return std::to_string(blockId) + "_" + std::to_string(operationId);
}
bool OperationIdentifier::operator<(const OperationIdentifier& rhs) const {
    if (blockId < rhs.blockId)
        return true;
    if (rhs.blockId < blockId)
        return false;
    return operationId < rhs.operationId;
}
bool OperationIdentifier::operator>(const OperationIdentifier& rhs) const { return rhs < *this; }
bool OperationIdentifier::operator<=(const OperationIdentifier& rhs) const { return !(rhs < *this); }
bool OperationIdentifier::operator>=(const OperationIdentifier& rhs) const { return !(*this < rhs); }
}// namespace NES::Nautilus::IR::Operations
