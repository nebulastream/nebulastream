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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSINKDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>

namespace NES::Statistic {

// Necessary for choosing the correct sink format
enum class StatisticSinkFormatType : uint8_t { COUNT_MIN, HLL };

/**
 * @brief Descriptor for the StatisticSink. Merely stores the StatisticSinkFormatType
 */
class StatisticSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method for creating the StatisticSinkDescriptor
     * @param sinkFormatType
     * @param numberOfOrigins
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr create(StatisticSinkFormatType sinkFormatType, uint64_t numberOfOrigins = 1);

    std::string toString() const override;
    bool equal(const SinkDescriptorPtr& other) override;
    StatisticSinkFormatType getSinkFormatType() const;

  private:
    explicit StatisticSinkDescriptor(StatisticSinkFormatType sinkFormatType, uint64_t numberOfOrigins);
    StatisticSinkFormatType sinkFormatType;
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSINKDESCRIPTOR_HPP_
