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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_COUNTMINDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_COUNTMINDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>

namespace NES::Experimental::Statistics {

class CountMinDescriptor : public WindowStatisticDescriptor {
  public:
    /**
     *
     * @param logicalSourceName the logical source name over which we wish to generate countMin sketches
     * @param fieldName the field name over which we wish to generate countMin sketches
     * @param timestampField the timestamp field which determines the window/sketch of a tuple
     * @param depth the depth of the count min sketches
     * @param windowSize the window size of the countMin sketches
     * @param slideFactor the slide factor of the CountMin sketches
     * @param width the width of the CM Sketch that we wish to create
     */
    explicit CountMinDescriptor(const std::string& logicalSourceName,
                                const std::string& fieldName,
                                const std::string& timestampField,
                                uint64_t depth,
                                uint64_t windowSize,
                                uint64_t slideFactor,
                                uint64_t width);

    /**
     * @return a string representation of the desc
     */
    std::string toString() const override;

    /**
     * @brief checks if two statisticOperators are equal
     * @param statisticDescriptor a statistic descriptor which holds specific values to the statistic
     * @return true if they are equal
     */
    bool operator==(WindowStatisticDescriptor& statisticDescriptor) override;

    /**
     * @brief adds specific meta fields to a schema, such that
     * @param schema the schema that will be modified
     */
    void addStatisticFields(SchemaPtr schema) override;

    /**
     * @return returns the width of the sketch
     */
    [[nodiscard]] uint64_t getWidth() const;

  private:
    uint64_t width;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_COUNTMINDESCRIPTOR_HPP_
