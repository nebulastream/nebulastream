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

#ifndef NES_PROXYCALLOPERATION_HPP
#define NES_PROXYCALLOPERATION_HPP

#include "Experimental/NESIR/Operations/Operation.hpp"
#include <vector>

namespace NES::ExecutionEngine::Experimental::IR::Operations {
class ProxyCallOperation : public Operation {
  public:
    ProxyCallOperation(ProxyCallType proxyCallType, std::string functionName, std::string identifier, std::vector<std::string> inputArgNames,
                       std::vector<Operation::BasicType> inputArgTypes, Operation::BasicType resultType);
    ~ProxyCallOperation() override = default;

    ProxyCallType getProxyCallType();
    const std::string& getFunctionName() const;
    void setFunctionName(const std::string& functionName);
    std::string getIdentifier();
    std::vector<std::string> getInputArgNames();
    std::vector<Operation::BasicType> getInputArgTypes();
    Operation::BasicType getResultType();
    
    std::string toString() override;

  private:
    ProxyCallType proxyCallType;
    std::string functionName;
    std::string identifier;
    std::vector<std::string> inputArgNames; // We need this to get the correct input args from the value map.
    std::vector<Operation::BasicType> inputArgTypes;
    Operation::BasicType resultType;
};
}// namespace NES
#endif//NES_PROXYCALLOPERATION_HPP
