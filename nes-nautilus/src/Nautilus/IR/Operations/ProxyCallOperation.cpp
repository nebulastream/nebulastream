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

#include "Nautilus/IR/Operations/Operation.hpp"
#include <Nautilus/IR/Operations/ProxyCallOperation.hpp>
#include <utility>
#include <vector>

namespace NES::Nautilus::IR::Operations {
ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType,
                                       OperationIdentifier identifier,
                                       std::vector<OperationRef> inputArguments,
                                       Types::StampPtr resultType)
    : Operation(Operation::OperationType::ProxyCallOp, std::move(identifier), resultType), proxyCallType(proxyCallType),
      inputArguments(std::move(inputArguments)) {}

ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType,
                                       std::string functionSymbol,
                                       void* functionPtr,
                                       OperationIdentifier identifier,
                                       std::vector<OperationRef> inputArguments,
                                       Types::StampPtr resultType)
    : Operation(Operation::OperationType::ProxyCallOp, identifier, resultType), proxyCallType(proxyCallType),
      mangedFunctionSymbol(functionSymbol), functionPtr(functionPtr), inputArguments(std::move(inputArguments)) {}

Operation::ProxyCallType ProxyCallOperation::getProxyCallType() const { return proxyCallType; }
const std::vector<OperationRef>& ProxyCallOperation::getInputArguments() const { return inputArguments; }

std::string ProxyCallOperation::toString() const {
    std::string baseString = "";
    if (!identifier.empty()) {
        baseString = identifier + " = ";
    }
    baseString = baseString + getFunctionSymbol() + "(";
    if (!inputArguments.empty()) {
        baseString += inputArguments[0]->getIdentifier();
        for (int i = 1; i < (int) inputArguments.size(); ++i) {
            baseString += ", " + inputArguments.at(i)->getIdentifier();
        }
    }
    return baseString + ")";
}
std::string ProxyCallOperation::getFunctionSymbol() const { return mangedFunctionSymbol; }

void* ProxyCallOperation::getFunctionPtr() const { return functionPtr; }

}// namespace NES::Nautilus::IR::Operations
