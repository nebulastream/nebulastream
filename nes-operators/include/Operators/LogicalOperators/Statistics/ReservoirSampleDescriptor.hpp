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

#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_RESERVOIRSAMPLEDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_RESERVOIRSAMPLEDESCRIPTOR_HPP_

namespace NES::Experimental::Statistics {

class ReservoirSampleDescriptor : public WindowStatisticDescriptor {
  public:

    /**
     * @param logicalSourceName the logical source name over which we wish to generate reservoirs
     * @param fieldName the field name over which we wish to generate reservoirs
     * @param timestampField the timestamp field which determines the window/reservoir of a tuple
     * @param depth the depth of the reservoirs
     * @param windowSize the window size of the reservoirs
     * @param slideFactor the slide factor of the reservoirs
     */
    ReservoirSampleDescriptor(const std::string& logicalSourceName,
                              const std::string& fieldName,
                              const std::string& timestampField,
                              uint64_t depth,
                              uint64_t windowSize,
                              uint64_t slideFactor);

    /**
     * @return a string version of the reservoir sample desc
     */
    std::string toString() const override;

    /**
     * @brief checks if two statisticOperators/reservoirs are equal
     * @param statisticDescriptor a statistic descriptor which holds specific values to the statistic
     * @return true if they are equal
     */
    bool operator==(WindowStatisticDescriptor& statisticsDescriptor) override;

    /**
     * @brief adds specific meta fields to a schema, in this scenario however none
     * @param schema the schema that will be modified
     */
    void addStatisticFields(SchemaPtr schema) override;
};
}
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_RESERVOIRSAMPLEDESCRIPTOR_HPP_
