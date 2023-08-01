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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_FLATMAPUDFLOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_FLATMAPUDFLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/UDFLogicalOperator.hpp>

namespace NES {

/**
 * Logical operator node for a flat map operation which uses a UDF.
 *
 * The operation completely replaces the stream tuple based on the result of the UDF method. Therefore, the output schema is
 * determined by the UDF method signature.
 */
class FlatMapUDFLogicalOperatorNode : public UDFLogicalOperator {
  public:
    /**
     * @brief Construct a FlatMapUdfLogicalOperatorNode.
     * @param udfDescriptor The descriptor of the UDF used in the map operation.
     * @param id The ID of the operator.
     */
    FlatMapUDFLogicalOperatorNode(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor, OperatorId id);

    std::string toString() const override;
    OperatorNodePtr copy() override;
    [[nodiscard]] bool equal(const NodePtr& other) const override;
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_FLATMAPUDFLOGICALOPERATORNODE_HPP_
