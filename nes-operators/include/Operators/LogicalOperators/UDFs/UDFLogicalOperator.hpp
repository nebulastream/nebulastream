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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFLOGICALOPERATOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFLOGICALOPERATOR_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {
namespace Catalogs::UDF {
class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;
}// namespace Catalogs::UDF

/**
 * Logical operator node for a udf. This class acts as a parent class for any udf logical operator node
 */
class UDFLogicalOperator : public LogicalUnaryOperatorNode {

  public:
    /**
     * Construct a UDFLogicalOperator.
     * @param udfDescriptor The descriptor of the UDF used in the map operation.
     * @param id The ID of the operator.
     */
    UDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id);

    /**
     * @see LogicalOperatorNode#inferStringSignature
     */
    void inferStringSignature() override;

    /**
     * Getter for the UDF descriptor.
     * @return The descriptor of the UDF used in the map operation.
     */
    Catalogs::UDF::UDFDescriptorPtr getUDFDescriptor() const;

    /**
     * @see LogicalUnaryOperatorNode#inferSchema
     *
     * Sets the output schema contained in the UDFDescriptor as the output schema of the operator.
     */
    bool inferSchema() override;

    /**
     * @see OperatorNode#copy
     */
    virtual OperatorNodePtr copy() override = 0;

    /**
     * Two MapUdfLogicalOperatorNode are equal when the wrapped UDFDescriptor are equal.
     */
    [[nodiscard]] virtual bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;

  protected:
    const Catalogs::UDF::UDFDescriptorPtr udfDescriptor;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_UDFLOGICALOPERATOR_HPP_