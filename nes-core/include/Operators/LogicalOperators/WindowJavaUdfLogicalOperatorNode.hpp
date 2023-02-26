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

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/JavaUdfLogicalOperator.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

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
                                     Windowing::WindowTypePtr windowType,
                                     Windowing::DistributionCharacteristicPtr distributionType,
                                     std::vector<FieldAccessExpressionNodePtr> onKey,
                                     uint64_t allowedLateness, OriginId originId, OperatorId id);

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
     * Two WindowJavaUdfLogicalOperatorNode are equal when the JavaUdfDescriptor are equal.
     */
    bool equal(const NodePtr &rhs) const override;

    /**
     * @see Node#isIdentical
     */
    bool isIdentical(const NodePtr &rhs) const override;

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
    std::vector<FieldAccessExpressionNodePtr> getKeys() const;

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
    Windowing::WindowTypePtr windowType;
    Windowing::DistributionCharacteristicPtr distributionType;
    std::vector<FieldAccessExpressionNodePtr> onKey;
    uint64_t allowedLateness;
    OriginId originId;
};

} // NES

#endif //NES_WINDOWJAVAUDFLOGICALOPERATORNODE_HPP
