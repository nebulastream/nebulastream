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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_SYNOPSESSTATISTIC_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_SYNOPSESSTATISTIC_HPP_

#include <Statistics/Statistic.hpp>

namespace NES::Statistic {

/**
 * @brief In the future, we might can/must/have some shared functionality between all Statistics that are a Synopsis.
 * For now, it is purely a placeholder
 */
class SynopsesStatistic : public Statistic {
  public:
    /**
     * @brief Merges this Synopsis with other. The actual implementation heavily depends on the underlying synopsis
     * @param other
     */
    virtual void merge(const SynopsesStatistic& other) = 0;

  protected:
    SynopsesStatistic(const Windowing::TimeMeasure& startTs, const Windowing::TimeMeasure& endTs, uint64_t observedTuples);
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_SYNOPSESSTATISTIC_HPP_
