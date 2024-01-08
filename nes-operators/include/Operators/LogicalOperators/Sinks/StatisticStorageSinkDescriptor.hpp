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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSTORAGESINKDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSTORAGESINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

/**
 * @brief descriptor that defines the behavior of the statistic storage sink
 */
class StatisticStorageSinkDescriptor : public SinkDescriptor {
  public:
    /**
     * @brief creates a shared pointer to a specified version of a StatisticStorageSinkDesc. This function can be called via the
     * @param statisticCollectorType the type of statistic object that the sink expects, from which it derives the output schema
     * @param numberOfOrigins number of origins of a given query
     * @return a abstract SinkDescriptorPtr
     */
    static SinkDescriptorPtr create(StatisticCollectorType statisticCollectorType,
                                    uint64_t numberOfOrigins = 1);

    virtual ~StatisticStorageSinkDescriptor() = default;

    /**
     * @return a string describing the descriptor
     */
    std::string toString() const override;

    /**
     * @brief checks if two Sink Descriptors are both StatisticStorageSinkDescriptors and of the same type
     * @param other another pointer to a sink descriptor
     * @return true when equal, otherwise false
     */
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    /**
     * @return the type of the statisticCollector that is meant to be written to the storage
     */
    StatisticCollectorType getStatisticCollectorType() const;
  private:
    /**
     * @brief the constructor of the StatisticStorageSinkDesc
     * @param statisticCollectorType the type of statistic obj the sink will expect
     * @param numberOfOrigins the number of origins
     */
    StatisticStorageSinkDescriptor(StatisticCollectorType statisticCollectorType,
                                   uint64_t numberOfOrigins = 1);

    StatisticCollectorType statisticCollectorType;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_STATISTICSTORAGESINKDESCRIPTOR_HPP_
