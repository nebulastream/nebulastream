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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_RESERVOIRSAMPLEDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_RESERVOIRSAMPLEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>

namespace NES::Statistic {

class ReservoirSampleDescriptor : public WindowStatisticDescriptor {
  public:

    static constexpr auto DEFAULT_SAMPLE_SIZE = 100;

    /**
     * @brief Creates a ReservoirSampleDescriptor with the DEFAULT_SAMPLE_SIZE and keepOnlyRequiredField = false
     * @param field
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field);

    /**
     * @brief Creates a ReservoirSampleDescriptor with the DEFAULT_SAMPLE_SIZE
     * @param field: Over which field to create the synopsis
     * @param keepOnlyRequiredField: If true, only the required field is kept
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, bool keepOnlyRequiredField);

    /**
     * @brief Creates a ReservoirSampleDescriptor
     * @param field: Over which field to create the synopsis
     * @param width: Number of samples to keep in the reservoir
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, uint64_t width, bool keepOnlyRequiredField);

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false.
     */
    bool equal(const WindowStatisticDescriptorPtr& rhs) const override;

    /**
     * @brief Adds the fields special to a Sample
     * @param inputSchema
     * @param outputSchema
     * @param qualifierNameWithSeparator
     */
    void addDescriptorFields(Schema& inputSchema, Schema& outputSchema, const std::string& qualifierNameWithSeparator) override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() override;

    /**
     * @brief Gets keepOnlyRequiredField
     * @return boolean
     */
    bool isKeepOnlyRequiredField() const;

    /**
     * @brief Getter for the type of the synopsis
     * @return StatisticSynopsisType
     */
    StatisticSynopsisType getType() const override;

    /**
     * @brief Virtual destructor
     */
    virtual ~ReservoirSampleDescriptor();

  private:
    /**
     * @brief Private constructor for a ReservoirSampleDescriptor
     * @param field: Over which field to create the synopsis
     * @param sampleSize: Number of samples to keep in the reservoir
     * @param keepOnlyRequiredField: If true, only the required field is kept
     */
    ReservoirSampleDescriptor(const FieldAccessExpressionNodePtr& field, uint64_t sampleSize, bool keepOnlyRequiredField);


    bool keepOnlyRequiredField;
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_RESERVOIRSAMPLEDESCRIPTOR_HPP_
