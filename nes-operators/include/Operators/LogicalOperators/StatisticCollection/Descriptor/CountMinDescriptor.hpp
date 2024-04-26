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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_COUNTMINDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_COUNTMINDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <cstdint>

namespace NES::Statistic {

/**
 * @brief Descriptor for a CountMin sketch
 */
class CountMinDescriptor : public WindowStatisticDescriptor {
  public:
    static constexpr auto DEFAULT_RELATIVE_ERROR = 0.05;
    static constexpr auto DEFAULT_ERROR_PROBABILITY = 0.05;

    /**
     * @brief Creates a CountMinDescriptor
     * @param field: Over which field to create the synopsis
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field);

    /**
     * @brief Creates a CountMinDescriptor
     * @param field: Over which field to create the synopsis
     * @param error: Relative error
     * @param probability: Probability that the error occurs
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, double error, double probability);

    /**
     * @brief Creates a CountMinDescriptor
     * @param field: Over which field to create the synopsis
     * @param width: Number of columns for the CountMin sketch
     * @param depth: Number of rows for the CountMin sketch
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, uint64_t width, uint64_t depth);

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false
     */
    bool equal(const WindowStatisticDescriptorPtr& rhs) const override;

    /**
     * @brief Adds the fields special to a CountMin descriptor
     * @param outputSchema
     * @param qualifierNameWithSeparator
     */
    void addDescriptorFields(Schema& outputSchema, const std::string& qualifierNameWithSeparator) override;

    /**
     * @brief Getter for the depth
     * @return uint64_t
     */
    uint64_t getDepth() const;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() override;

    /**
     * @brief Virtual destructor
     */
    ~CountMinDescriptor() override;

  private:
    /**
     * @brief Private constructor for creating a CountMinDescriptor
     * @param field: Over which field to create the synopsis
     * @param width: Number of columns for the CountMin sketch
     * @param depth: Number of rows for the CountMin sketch
     */
    CountMinDescriptor(const FieldAccessExpressionNodePtr& field, uint64_t width, uint64_t depth);

    uint64_t depth;
};

}// namespace NES::Statistic

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_COUNTMINDESCRIPTOR_HPP_
