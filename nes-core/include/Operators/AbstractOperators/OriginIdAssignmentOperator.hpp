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
#ifndef NES_NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
#define NES_NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {
/**
 * @brief An operator, which creates an initializes a new origin id.
 * This is usually a an Source, Window, or Join Operator.
 */
class OriginIdAssignmentOperator : public virtual OperatorNode {
  public:
    OriginIdAssignmentOperator(OperatorId operatorId, OriginId originId = INVALID_ORIGIN_ID);
    virtual std::vector<OriginId> getOutputOriginIds() override;

    void setOriginId(OriginId originId);
    OriginId getOriginId();

  protected:
    OriginId originId;
};
}// namespace NES
// namespace NES
#endif//NES_NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
