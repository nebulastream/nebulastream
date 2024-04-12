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

namespace NES::Nautilus::IR::Operations {
Operation::Operation(OperationType opType, OperationIdentifier identifier, Types::StampPtr stamp)
    : opType(opType), identifier(identifier), stamp(stamp) {}
Operation::Operation(OperationType opType, Types::StampPtr stamp) : opType(opType), identifier(""), stamp(stamp) {}
Operation::OperationType Operation::getOperationType() const { return opType; }
OperationIdentifier Operation::getIdentifier() const { return identifier; }
const Types::StampPtr& Operation::getStamp() const { return stamp; }

void Operation::addUsage(const Operation& operation) { usages.emplace_back(&operation); }

const std::vector<gsl::not_null<const Operation*>>& Operation::getUsages() const { return usages; }

}// namespace NES::Nautilus::IR::Operations
