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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_DDSKETCHDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_DDSKETCHDESCRIPTOR_HPP_
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <cstdint>
namespace NES::Statistic {
class DDSketchDescriptor : public WindowStatisticDescriptor {
  public:
    static constexpr auto DEFAULT_RELATIVE_ERROR = 0.05;
    static constexpr auto NUMBER_OF_PRE_ALLOCATED_BUCKETS = 1024;


    /**
     * @brief Creates a DDSketchDescriptor
     * @param field: Over which field to create the synopsis
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field);

    /**
     * @brief Creates a DDSketchDescriptor
     * @param field: Over which field to create the synopsis
     * @param error: Relative error
     * @param numberOfPreAllocatedBuckets: Number of pre-allocated buckets for each DD-Sketch
     * @return WindowStatisticDescriptorPtr
     */
    static WindowStatisticDescriptorPtr create(FieldAccessExpressionNodePtr field, double error, uint64_t numberOfPreAllocatedBuckets);

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false
     */
    bool equal(const WindowStatisticDescriptorPtr& rhs) const override;

    /**
     * @brief Adds the fields special to a DD-Sketch descriptor
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
     * @brief Virtual destructor
     */
    ~DDSketchDescriptor() override;

    /**
     * @brief Calculates the gamma value
     * @return double
     */
    double calculateGamma() const;

    /**
     * @brief Getter for the relative error
     * @return double
     */
    double getRelativeError() const;

    /**
     * @brief Getter for the calculation of the log floor index expressions
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getCalcLogFloorIndexExpressions() const;

    /**
     * @brief Getter for the number of pre-allocated buckets
     * @return uint64_t
     */
    uint64_t getNumberOfPreAllocatedBuckets() const;

    /**
     * @brief Getter for the calculation of < 0
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getGreaterThanZeroExpression() const;

    /**
     * @brief Getter for the calculation of > 0
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getLessThanZeroExpression() const;
    void inferStamps(const SchemaPtr& inputSchema) override;

    /**
     * @brief Getter for the type of the statistic
     * @return StatisticSynopsisType
     */
    StatisticSynopsisType getType() const override;

  private:
     /**
      * @brief Private constructor for creating a DD-Sketch descriptor
      * @param field: Over which field to create the synopsis
      * @param relativeAccuracy: The relative accurary for each DD-Sketch
      * @param numberOfPreAllocatedBuckets: Number of pre-allocated buckets for each DD-Sketch
      */
    DDSketchDescriptor(FieldAccessExpressionNodePtr field, double relativeAccuracy, uint64_t numberOfPreAllocatedBuckets);
    double relativeError;
    ExpressionNodePtr calcLogFloorIndexExpressions, greaterThanZeroExpression, lessThanZeroExpression;
};
} // namespace NES::Statistic
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_DESCRIPTOR_DDSKETCHDESCRIPTOR_HPP_
