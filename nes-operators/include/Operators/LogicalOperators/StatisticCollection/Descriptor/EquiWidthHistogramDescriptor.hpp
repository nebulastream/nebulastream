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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_EQUIWIDTHHISTOGRAMDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_EQUIWIDTHHISTOGRAMDESCRIPTOR_HPP_
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
namespace NES::Statistic {
class EquiWidthHistogramDescriptor : public WindowStatisticDescriptor {
  public:
    static constexpr auto DEFAULT_BIN_WIDTH = 10;

    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field);
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, uint64_t binWidth);
    bool equal(const WindowStatisticDescriptorPtr& rhs) const override;
    void addDescriptorFields(Schema& inputSchema, Schema& outputSchema, const std::string& qualifierNameWithSeparator) override;
    std::string toString() override;
    ~EquiWidthHistogramDescriptor() override;
    /**
     * @brief Getter for the type of the synopsis
     * @return StatisticSynopsisType
     */
    StatisticSynopsisType getType() const override;

  private:
    EquiWidthHistogramDescriptor(FieldAccessExpressionNodePtr field, uint64_t binWidth);
};
} // namespace NES::Statistic
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_EQUIWIDTHHISTOGRAMDESCRIPTOR_HPP_
