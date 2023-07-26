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
#ifndef NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {
/**
 * @brief An operator, which creates an initializes a new origin id.
 * This are usually operators that create and emit new data records,
 * e.g., a Source, Window aggregation, or Join Operator.
 * Operators that only modify or select an already existing record, e.g.,
 * Filter or Map, dont need to assign new origin ids.
 */
class OriginIdAssignmentOperator : public virtual OperatorNode {
  public:
    /**
     * @brief Constructor for the origin id operator
     * @param operatorId OperatorId
     * @param originId if not set INVALID_ORIGIN_ID is default
     */
    OriginIdAssignmentOperator(OperatorId operatorId, OriginId originId = INVALID_ORIGIN_ID);

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    virtual const std::vector<OriginId> getOutputOriginIds() const override;

    /**
     * @brief Sets the origin id, which is used from this operator as an output
     * @param originId
     */
    virtual void setOriginId(OriginId originId);

    /**
     * @brief Get the origin id
     * @return OriginId
     */
    OriginId getOriginId() const;

  protected:
    OriginId originId;
};
}// namespace NES
// namespace NES
#endif// NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ORIGINIDASSIGNMENTOPERATOR_HPP_
