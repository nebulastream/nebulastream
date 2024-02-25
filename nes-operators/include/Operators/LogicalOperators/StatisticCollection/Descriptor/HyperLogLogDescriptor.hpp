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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SYNOPSES_HYPERLOGLOGDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SYNOPSES_HYPERLOGLOGDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <cstdint>

namespace NES::Statistic {

class HyperLogLogDescriptor : public WindowStatisticDescriptor {
  public:
    static constexpr auto DEFAULT_RELATIVE_ERROR = 0.05;

    /**
     * @brief Creates a HyperLogLogDescriptor
     * @param field: Over which field to create the synopsis
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field);

    /**
     * @brief Creates a HyperLogLogDescriptor
     * @param field: Over which field to create the synopsis
     * @param error: Relative error for the estimation
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, double error);

    /**
     * @brief Creates a HyperLogLogDescriptor
     * @param field: Over which field to create the synopsis
     * @param width: Number of bits for the HyperLogLog
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, uint64_t width);

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false.
     */
    bool equal(const WindowStatisticDescriptorPtr& rhs) const override;

    /**
     * @brief Adds the fields special to a HyperLogLog
     * @param outputSchema
     * @param qualifierNameWithSeparator
     */
    void addDescriptorFields(Schema& outputSchema, const std::string& qualifierNameWithSeparator) override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() override;

    /**
     * @brief Virtual destructor
     */
    virtual ~HyperLogLogDescriptor();

  private:
    /**
     * @brief Private constructor for a HyperLogLogDescriptor
     * @param field: Over which field to create the synopsis
     * @param width: Number of bits for the HyperLogLog
     */
    HyperLogLogDescriptor(const FieldAccessExpressionNodePtr& field, uint64_t width);
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_SYNOPSES_HYPERLOGLOGDESCRIPTOR_HPP_
