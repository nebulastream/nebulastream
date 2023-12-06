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
     * @param width the width/size of the reservoir sample
     */
    ReservoirSampleDescriptor(uint64_t width);

    /**
     * @brief checks if two statisticOperators/reservoirs are equal
     * @param statisticDescriptor a statistic descriptor which holds specific values to the statistic
     * @return true if they are equal
     */
    bool operator==(WindowStatisticDescriptor& statisticsDescriptor) override;

    /**
     * @brief adds specific meta fields to a schema, such that
     * @param schema the schema that will be modified
     */
    void addStatisticFields(SchemaPtr schema) override;

    /**
     * @return the width/size of the sample
     */
    uint64_t getWidth() const;

  private:
    uint64_t width;
};
}
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_RESERVOIRSAMPLEDESCRIPTOR_HPP_
