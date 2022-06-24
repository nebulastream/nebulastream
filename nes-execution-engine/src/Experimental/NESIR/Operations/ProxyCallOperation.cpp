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

#include <Experimental/NESIR/Operations/ProxyCallOperation.hpp>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType,
                                       OperationIdentifier identifier,
                                       std::vector<OperationWPtr> inputArguments,
                                       PrimitiveStamp resultType)
    : Operation(Operation::ProxyCallOp, identifier, resultType), proxyCallType(proxyCallType),
      inputArguments(std::move(inputArguments)), resultType(resultType) {}

Operation::ProxyCallType ProxyCallOperation::getProxyCallType() { return proxyCallType; }
std::vector<OperationPtr> ProxyCallOperation::getInputArguments() {
    std::vector<OperationPtr> args;
    for (auto input : inputArguments) {
        args.emplace_back(input.lock());
    }
    return args;
}
PrimitiveStamp ProxyCallOperation::getResultType() { return resultType; }

std::string ProxyCallOperation::toString() {
    std::string baseString = "ProxyCallOperation_" + identifier + "(";
    if (inputArguments.size() > 0) {
        baseString += inputArguments[0].lock()->getIdentifier();
        for (int i = 1; i < (int) inputArguments.size(); ++i) {
            baseString += ", " + inputArguments.at(i).lock()->getIdentifier();
        }
    }
    return baseString + ")";
}

}// namespace NES::ExecutionEngine::Experimental::IR::Operations
