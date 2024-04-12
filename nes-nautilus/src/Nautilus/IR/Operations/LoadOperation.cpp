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
#include <Nautilus/IR/Operations/Operation.hpp>
#include <string>
#include <utility>

namespace NES::Nautilus::IR::Operations {

LoadOperation::LoadOperation(OperationIdentifier identifier, Operation& address, Types::StampPtr stamp)
    : Operation(OperationType::LoadOp, std::move(identifier), std::move(stamp)), address(address) {
    address.addUsage(*this);
}

const Operation& LoadOperation::getAddress() const { return address; }

std::string LoadOperation::toString() const { return identifier + " = load(" + getAddress().getIdentifier() + ")"; }

}// namespace NES::Nautilus::IR::Operations
