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

#ifndef NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_STATISTIC_HPP_
#define NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_STATISTIC_HPP_

#include <memory>

namespace NES::Experimental::Statistics {

class StatisticProbeParameter;
using StatisticProbeParameterPtr = std::shared_ptr<StatisticProbeParameter>;

class StatisticCollectorIdentifier;
using StatisticCollectorIdentifierPtr = std::shared_ptr<StatisticCollectorIdentifier>;
/**
 * @brief this class defines the general interface for all statistics such as count-min and the reservoir sample
 */
class Statistic {
  public:
    /**
     * @param statisticCollectorIdentifier the key that identifies a statistic
     * @param observedTuples the number of observed tuples by a statistic
     * @param depth the depth of the statistic
     */
    Statistic(StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
              uint64_t observedTuples,
              uint64_t depth);

    virtual ~Statistic() = default;

    /**
     * @brief a probe function that all statistics have to implement, defining how statistics are obtained from them
     * @param probeParameters a parameter defining the probe operation
     * @return a statistic
     */
    virtual double probe(StatisticProbeParameterPtr& probeParameters) = 0;

    /**
     * @return returns the identifier of the StatisticCollectorFormat, which holds information about where is was built and what kind of statistic obj it is
     */
    StatisticCollectorIdentifierPtr getStatisticCollectorIdentifier() const;

    /**
     * @return returns the number of observed tuples
     */
    uint64_t getObservedTuples() const;

    /**
     * @return returns the depth of the StatisticCollectorFormat
     */
    uint64_t getDepth() const;

    /**
     * @return returns the time at which tuples were first used to generate the StatisticCollectorFormat
     */
    uint64_t getStartTime() const;

    /**
     * @return returns the latest time at which tuples were used to generate the StatisticCollectorFormat
     */
    uint64_t getEndTime() const;

  private:
    StatisticCollectorIdentifierPtr statisticCollectorIdentifier;
    uint64_t observedTuples;
    uint64_t depth;
};
}// namespace NES::Experimental::Statistics
#endif//NES_NES_RUNTIME_INCLUDE_STATISTICS_STATISTICS_STATISTIC_HPP_
