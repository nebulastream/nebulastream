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

#ifndef NES_NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3QUERYSIGNATURECONTEXT_HPP_
#define NES_NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3QUERYSIGNATURECONTEXT_HPP_
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Util/QuerySignatureContext.hpp>

namespace NES::Optimizer {

class Z3QuerySignatureContext : public QuerySignatureContext {
  public:
    explicit Z3QuerySignatureContext(const z3::ContextPtr& context);
    QuerySignaturePtr createQuerySignatureForOperator(const OperatorNodePtr& operatorNode) const override;
    z3::ContextPtr getContext() const;
  private:
    const z3::ContextPtr context;
};

} // namespace NES::Optimizer


#endif//NES_NES_OPTIMIZER_INCLUDE_UTIL_QUERYSIGNATURES_Z3QUERYSIGNATURECONTEXT_HPP_
