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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALINTERVALJOINDESCRIPTOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALINTERVALJOINDESCRIPTOR_HPP_

#include <API/Schema.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <cstdint>

namespace NES::Join {

/**
* @brief Runtime definition of a join operator
*/
class LogicalIntervalJoinDescriptor {

  public:
    static LogicalIntervalJoinDescriptorPtr create(ExpressionNodePtr joinExpression,
                                                   Windowing::TimeCharacteristicPtr timeCharacteristic,
                                                   int64_t lowerBound,
                                                   int64_t upperBound);

    explicit LogicalIntervalJoinDescriptor(ExpressionNodePtr joinExpression,
                                           Windowing::TimeCharacteristicPtr timeCharacteristic,
                                           int64_t lowerBound,
                                           int64_t upperBound);

    /**
    * @brief Getter for join expression
    */
    ExpressionNodePtr getJoinExpression() const;

    /**
    * @brief Getter left schema
    */
    SchemaPtr getLeftSchema() const;

    /**
    * @brief Getter right schema
    */
    SchemaPtr getRightSchema() const;

    /**
     * @brief Getter timeCharacteristic
     */
    Windowing::TimeCharacteristicPtr getTimeCharacteristic() const;

    /**
    * @brief Getter lower interval bound
    */
    int64_t getLowerBound() const;

    /**
    * @brief Getter upper interval bound
    */
    int64_t getUpperBound() const;

    /**
    * @brief Update the left and right stream types upon type inference
    * @param leftSchema
    * @param rightSchema
    */
    void updateInputSchemas(SchemaPtr leftSchema, SchemaPtr rightSchema);

    /**
    * @brief Update the output stream type upon type inference
    * @param outputSchema : the type of the output stream
    */
    void updateOutputDefinition(SchemaPtr outputSchema);

    /**
    * @brief Getter of the output stream schema
    * @return the output stream schema
    */
    [[nodiscard]] SchemaPtr getOutputSchema() const;

    /**
     * creates a string representation of the interval join definition
     * @return
     */
    [[nodiscard]] std::string toString() const;

    /**
     * We store the interval start and end to write them into the joined tuples later as metadata
     * The following functions are used to update and get the interval start and end field names
     */
    void updateIntervalFields(std::string intervalStartFieldName, std::string intervalEndFieldName);

    const std::string& getIntervalStartFieldName() const;
    const std::string& getIntervalEndFieldName() const;

  private:
    ExpressionNodePtr joinExpression;
    SchemaPtr leftSchema{nullptr};
    SchemaPtr rightSchema{nullptr};
    SchemaPtr outputSchema{nullptr};
    Windowing::TimeCharacteristicPtr timeCharacteristic;
    int64_t lowerBound;
    int64_t upperBound;
    std::string intervalStartFieldName = "$start";
    std::string intervalEndFieldName = "$end";
};

using LogicalIntervalJoinDescriptorPtr = std::shared_ptr<LogicalIntervalJoinDescriptor>;

}// namespace NES::Join
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALINTERVALJOINDESCRIPTOR_HPP_
