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
ProxyCallOperation::ProxyCallOperation(ProxyCallType proxyCallType, std::string identifier, 
                                        std::vector<std::string> inputArgNames, std::vector<PrimitiveStamp> inputArgTypes,
                                       PrimitiveStamp resultType)
    : Operation(Operation::ProxyCallOp, resultType), proxyCallType(proxyCallType), identifier(identifier), inputArgNames(std::move(inputArgNames)),
      inputArgTypes(inputArgTypes), resultType(resultType) {}

    Operation::ProxyCallType ProxyCallOperation::getProxyCallType() { return proxyCallType; }
    std::string ProxyCallOperation::getIdentifier() { return identifier; }
    std::vector<std::string> ProxyCallOperation::getInputArgNames() { return inputArgNames; }
    std::vector<PrimitiveStamp> ProxyCallOperation::getInputArgTypes() { return inputArgTypes; }
    PrimitiveStamp ProxyCallOperation::getResultType() { return resultType; }

    std::string ProxyCallOperation::toString() {
        std::string baseString = "ProxyCallOperation_" + identifier + "(";
        if(inputArgNames.size() > 0) {
            baseString += inputArgNames[0];
            for(int i = 1; i < (int) inputArgNames.size(); ++i) { 
                baseString += ", " + inputArgNames.at(i);
            }
        }
        return baseString + ")";
    }

}// namespace NES
