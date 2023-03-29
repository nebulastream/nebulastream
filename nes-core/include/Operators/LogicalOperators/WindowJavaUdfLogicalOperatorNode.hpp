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

#ifndef NES_WINDOWJAVAUDFLOGICALOPERATORNODE_HPP
#define NES_WINDOWJAVAUDFLOGICALOPERATORNODE_HPP

#include <Operators/LogicalOperators/JavaUdfLogicalOperator.hpp>

namespace NES {

/**
 * Logical operator node for a window operation which uses a Java UDF.
 *
 * The operation completely replaces the stream tuple based on the result of the Java UDF method. Therefore, the output schema is
 * determined by the UDF method signature.
 */
class WindowJavaUdfLogicalOperatorNode : public JavaUdfLogicalOperator {
  public:
    /**
     * @brief Constructor for a WindowJavaUdfLogicalOperatorNode
     * @param javaUdfDescriptor
     * @param windowType
     * @param distributionType
     * @param onKey
     * @param allowedLateness
     * @param originId
     * @param id
     */
    WindowJavaUdfLogicalOperatorNode(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor,
                                     const Windowing::WindowTypePtr windowType,
                                     const Windowing::DistributionCharacteristicPtr distributionType,
                                     const std::vector<FieldAccessExpressionNodePtr> onKey,
                                     const uint64_t allowedLateness,
                                     const OriginId originId,
                                     const OperatorId id);

    /**
     * @brief To string method for the current node.
     * @return string
     */
    std::string toString() const override;

    /**
     * @brief Create a shallow copy of the operator by copying its operator properties but not its children or parent operator tree.
     * @return shallow copy of the operator
     */
    OperatorNodePtr copy() override;

    /**
     * Two WindowJavaUdfLogicalOperatorNode are equal when the JavaUdfDescriptor are equal.
     */
    bool equal(const NodePtr& rhs) const override;

    /**
     * @brief overrides the isIdentical from Node
     */
    bool isIdentical(const NodePtr& rhs) const override;

    /**
     * @brief getter of the window type
     * @return WindowType
     */
    Windowing::WindowTypePtr getWindowType() const;

    /**
     * @brief getter of the distribution type
     * @return DistributionCharacteristicPtr
     */
    Windowing::DistributionCharacteristicPtr getDistributionType() const;

    /**
     * @brief getter of all keys of this window
     * @return Vector of all key
     */
    const std::vector<FieldAccessExpressionNodePtr>& getKeys() const;

    /**
     * @brief getter of allowed lateness
     * @return AllowedLateness
     */
    uint64_t getAllowedLateness() const;

    /**
     * @brief getter of the origin id
     * @return OriginId
     */
    OriginId getOriginId() const;

    /**
     * @brief checks, if this window is keyed
     * @return bool
     */
    bool isKeyed() const;

  private:
    OperatorNodePtr copyInternal(std::shared_ptr<WindowJavaUdfLogicalOperatorNode>& copy);

    Windowing::WindowTypePtr windowType;
    Windowing::DistributionCharacteristicPtr distributionType;
    std::vector<FieldAccessExpressionNodePtr> onKey;
    uint64_t allowedLateness;

    OriginId originId;
};

}// namespace NES

#endif//NES_WINDOWJAVAUDFLOGICALOPERATORNODE_HPP
