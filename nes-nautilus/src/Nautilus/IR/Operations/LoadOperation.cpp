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

#include <Nautilus/IR/Operations/LoadOperation.hpp>

namespace NES::Nautilus::IR::Operations {

LoadOperation::LoadOperation(OperationIdentifier identifier, OperationPtr address, Types::StampPtr type)
    : Operation(OperationType::LoadOp, identifier, type), address(std::move(address)) {
    address->addUsage(this);
}

OperationPtr LoadOperation::getAddress() { return address.lock(); }
void LoadOperation::setAddress(OperationWPtr newAddress) { 
    this->address = newAddress;
 }

std::string LoadOperation::toString() { return identifier + " = load(" + getAddress()->getIdentifier() + ")"; }

}// namespace NES::Nautilus::IR::Operations
