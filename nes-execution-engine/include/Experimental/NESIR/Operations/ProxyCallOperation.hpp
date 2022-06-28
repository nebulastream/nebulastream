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
    ProxyCallOperation(ProxyCallType proxyCallType,
                       OperationIdentifier identifier,
                       std::vector<OperationWPtr> inputArguments,
                       PrimitiveStamp resultType);
    ProxyCallOperation(ProxyCallType proxyCallType,
                       std::string mangedFunctionSymbol,
                       void* functionPtr,
                       OperationIdentifier identifier,
                       std::vector<OperationWPtr> inputArguments,
                       PrimitiveStamp resultType);
    ~ProxyCallOperation() override = default;
    ProxyCallType getProxyCallType();
    std::vector<OperationPtr> getInputArguments();
    std::string getFunctionSymbol();
    std::string toString() override;
    void* getFunctionPtr();

  private:
    ProxyCallType proxyCallType;
    std::string mangedFunctionSymbol;
    void* functionPtr;
    std::string identifier;
    std::vector<OperationWPtr> inputArguments;
};
}// namespace NES::ExecutionEngine::Experimental::IR::Operations
#endif//NES_PROXYCALLOPERATION_HPP
