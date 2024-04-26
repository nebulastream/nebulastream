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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_WINDOWSTATISTICDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_WINDOWSTATISTICDESCRIPTOR_HPP_

#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicy.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/TriggerCondition.hpp>
#include <cstdint>

namespace NES::Statistic {

class WindowStatisticDescriptor;
using WindowStatisticDescriptorPtr = std::shared_ptr<WindowStatisticDescriptor>;

/**
 * @brief Abstract class for a WindowStatisticDescriptor
 */
class WindowStatisticDescriptor : public std::enable_shared_from_this<WindowStatisticDescriptor> {
  public:
    /**
     * @brief Constructor for a WindowStatisticDescriptor
     * @param field: Over which field to track the statistic
     */
    WindowStatisticDescriptor(const FieldAccessExpressionNodePtr& field, uint64_t width);

    virtual ~WindowStatisticDescriptor() = default;

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false.
     */
    virtual bool equal(const WindowStatisticDescriptorPtr& rhs) const = 0;

    /**
     * @brief Getter for the FieldAccessExpression over which the statistic is being collected
     * @return FieldAccessExpressionNodePtr
     */
    FieldAccessExpressionNodePtr getField() const;

    /**
     * @brief Getter for the width
     * @return uint64_t
     */
    uint64_t getWidth() const;

    /**
     * @brief Adds fields to the output schema specific to the WindowStatisticDescriptor
     * @param outputSchema
     * @param qualifierNameWithSeparator
     */
    virtual void addDescriptorFields(Schema& outputSchema, const std::string& qualifierNameWithSeparator) = 0;

    /**
     * @brief Infers all stamps for the descriptor
     * @param inputSchema
     */
    virtual void inferStamps(const SchemaPtr& inputSchema);

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    virtual std::string toString() = 0;

    /**
     * @brief Checks if the current WindowStatisticDescriptor is of type WindowStatisticDescriptorType
     * @tparam WindowStatisticDescriptorType
     * @return bool true if node is of WindowStatisticDescriptorType
     */
    template<class WindowStatisticDescriptorType>
    bool instanceOf() {
        if (dynamic_cast<WindowStatisticDescriptorType*>(this)) {
            return true;
        }
        return false;
    };

    /**
     * @brief Checks if the current WindowStatisticDescriptor is of type WindowStatisticDescriptorType
     * @tparam WindowStatisticDescriptorType
     * @return bool true if WindowStatisticDescriptor is of WindowStatisticDescriptorType
     */
    template<class WindowStatisticDescriptorType>
    bool instanceOf() const {
        if (dynamic_cast<WindowStatisticDescriptorType*>(this)) {
            return true;
        }
        return false;
    };

    /**
    * @brief Dynamically casts the WindowStatisticDescriptor to a WindowStatisticDescriptorType
    * @tparam WindowStatisticDescriptorType
    * @return returns a shared pointer of the WindowStatisticDescriptorType
    */
    template<class WindowStatisticDescriptorType>
    std::shared_ptr<WindowStatisticDescriptorType> as() {
        if (instanceOf<WindowStatisticDescriptorType>()) {
            return std::dynamic_pointer_cast<WindowStatisticDescriptorType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator to type.");
    }

    /**
    * @brief Dynamically casts the node to a WindowStatisticDescriptorType
    * @tparam WindowStatisticDescriptorType
    * @return returns a shared pointer of the WindowStatisticDescriptorType
    */
    template<class WindowStatisticDescriptorType>
    std::shared_ptr<WindowStatisticDescriptorType> as() const {
        if (instanceOf<WindowStatisticDescriptorType>()) {
            return std::dynamic_pointer_cast<WindowStatisticDescriptorType>(this->shared_from_this());
        }
        throw std::logic_error("We performed an invalid cast of operator to type.");
    }

  protected:
    const FieldAccessExpressionNodePtr field;
    const uint64_t width;
};

}// namespace NES::Statistic

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_WINDOWSTATISTICDESCRIPTOR_HPP_
