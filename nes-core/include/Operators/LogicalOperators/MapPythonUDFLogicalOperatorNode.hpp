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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#ifndef NES_MAPPYTHONUDFLOGICALOPERATORNODE_HPP
#define NES_MAPPYTHONUDFLOGICALOPERATORNODE_HPP

#include <Operators/LogicalOperators/PythonUDFLogicalOperator.hpp>

namespace NES {

/**
 * Logical operator node for a map operation which uses a Python UDF.
 *
 * The operation completely replaces the stream tuple based on the result of the Python UDF method. Therefore, the output schema is
 * determined by the UDF method signature.
 */
class MapPythonUDFLogicalOperatorNode : public PythonUDFLogicalOperator {
  public:
    /**
     * Construct a MapUdfLogicalOperatorNode.
     * @param descriptor The descriptor of the Python UDF used in the map operation.
     * @param id The ID of the operator.
     */
    MapPythonUDFLogicalOperatorNode(const Catalogs::UDF::PythonUDFDescriptorPtr descriptor, OperatorId id);

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
     * Two MapUdfLogicalOperatorNode are equal when the wrapped PythonUDFDescriptor are equal.
     */
    [[nodiscard]] bool equal(const NodePtr& other) const override;

    /**
     * @see Node#isIdentical
     */
    [[nodiscard]] bool isIdentical(const NodePtr& other) const override;
};
}// namespace NES

#endif//NES_MAPPYTHONUDFLOGICALOPERATORNODE_HPP
#endif// NAUTILUS_PYTHON_UDF_ENABLED
