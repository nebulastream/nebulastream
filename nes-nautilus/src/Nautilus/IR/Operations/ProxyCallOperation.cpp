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

#include "Util/Logger/Logger.hpp"
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>

namespace NES::Nautilus::IR::Operations {
ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType,
                                       OperationIdentifier identifier,
                                       std::vector<OperationPtr> inputArguments,
                                       Types::StampPtr resultType)
    : Operation(Operation::OperationType::ProxyCallOp, identifier, resultType), proxyCallType(proxyCallType),
      inputArguments(std::move(inputArguments)) {}

ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType,
                                       std::string functionSymbol,
                                       void* functionPtr,
                                       OperationIdentifier identifier,
                                       std::vector<OperationPtr> inputArguments,
                                       Types::StampPtr resultType)
    : Operation(Operation::OperationType::ProxyCallOp, identifier, resultType), proxyCallType(proxyCallType),
      mangedFunctionSymbol(functionSymbol), functionPtr(functionPtr), inputArguments(std::move(inputArguments)) {}

Operation::ProxyCallType ProxyCallOperation::getProxyCallType() { return proxyCallType; }

std::vector<OperationPtr> ProxyCallOperation::getInputArguments() const {
    // std::vector<OperationPtr> lockedArguments;
    // for (auto& arg : this->inputArguments) {
        // NES_DEBUG("use count: {}", arg.use_count());
        // NES_DEBUG("is expired: {}", arg.expired());
        // lockedArguments.emplace_back(arg.lock());
    // }
    // return lockedArguments;
    return inputArguments;
}
void ProxyCallOperation::setInputArguments(std::vector<OperationPtr> newInputArguments) {
    this->inputArguments = newInputArguments;
}

std::string ProxyCallOperation::toString() {
    std::string baseString = "";
    if (!identifier.empty()) {
        baseString = identifier + " = ";
    }
    baseString = baseString + getFunctionSymbol() + "(";
    if (!inputArguments.empty()) {
        baseString += inputArguments[0]->getIdentifier();
        // baseString += inputArguments[0].lock()->getIdentifier();
        for (int i = 1; i < (int) inputArguments.size(); ++i) {
            baseString += ", " + inputArguments.at(i)->getIdentifier();
            // baseString += ", " + inputArguments.at(i).lock()->getIdentifier();
        }
    }
    return baseString + ")";
}
std::string ProxyCallOperation::getFunctionSymbol() { return mangedFunctionSymbol; }

void* ProxyCallOperation::getFunctionPtr() { return functionPtr; }

}// namespace NES::Nautilus::IR::Operations
