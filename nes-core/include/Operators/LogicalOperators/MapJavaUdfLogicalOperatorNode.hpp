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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

namespace Catalogs::UDF {
class JavaUdfDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUdfDescriptor>;
}// namespace Catalogs::UDF

/**
 * Logical operator node for a map operation which uses a Java UDF.
 *
 * The operation completely replaces the stream tuple based on the result of the Java UDF method. Therefore, the output schema is
 * determined by the UDF method signature.
 */
class MapJavaUdfLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    /**
     * Construct a MapUdfLogicalOperatorNode.
     * @param javaUdfDescriptor The descriptor of the Java UDF used in the map operation.
     * @param id The ID of the operator.
     */
    MapJavaUdfLogicalOperatorNode(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id);

    /**
     * Getter for the Java UDF descriptor.
     * @return The descriptor of the Java UDF used in the map operation.
     */
    Catalogs::UDF::JavaUdfDescriptorPtr getJavaUdfDescriptor() const;

    /**
     * @see LogicalUnaryOperatorNode#inferSchema
     *
     * Sets the output schema contained in the JavaUdfDescriptor as the output schema of the operator.
     */
    bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) override;

    /**
     * @see LogicalOperatorNode#inferStringSignature
     */
    void inferStringSignature() override;

    /**
     * @see Node#toString
     */
    std::string toString() const override;

    /**
     * @see OperatorNode#copy
     */
    OperatorNodePtr copy() override;

    /**
     * @see Node#equal
     *
     * Two MapUdfLogicalOperatorNode are equal when the wrapped JavaUdfDescriptor are equal.
     */
    [[nodiscard]] bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;

  private:
    const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor;
};
using MapJavaUdfLogicalOperatorNodePtr = std::shared_ptr<MapJavaUdfLogicalOperatorNode>;
}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_MAPJAVAUDFLOGICALOPERATORNODE_HPP_
