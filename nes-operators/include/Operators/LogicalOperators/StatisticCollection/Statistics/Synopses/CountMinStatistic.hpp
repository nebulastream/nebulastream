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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_COUNTMINSTATISTIC_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_COUNTMINSTATISTIC_HPP_

#include <Operators/LogicalOperators/StatisticCollection/Statistics/Synopses/SynopsesStatistic.hpp>


namespace NES::Statistic {

/**
 * @brief A statistic that is represented by a Count-Min sketch. In
 */
class CountMinStatistic : public SynopsesStatistic {
  public:
    /**
     * @brief Factory method for creating a CountMinStatistic
     * @param startTs: Timestamp of the first tuple for that this CountMin was created
     * @param endTs: Timestamp of the last tuple for that this CountMin was created
     * @param observedTuples: Number of tuples seen during the creation
     * @param width: Number of columns
     * @param depth: Number of rows
     * @param countMinDataString: String that stores the CountMin data, so the underlying 2-D array of counters
     * @return StatisticPtr
     */
    static StatisticPtr create(const Windowing::TimeMeasure& startTs,
                               const Windowing::TimeMeasure& endTs,
                               uint64_t observedTuples,
                               uint64_t width,
                               uint64_t depth,
                               uint64_t numberOfBitsInKey,
                               const std::string_view countMinDataString);

    /**
     * @brief Creates a CountMin for the given startTs, endTs, width, and depth. The #observedTuples and all counters are set to 0
     * @param startTs
     * @param endTs
     * @param width
     * @param depth
     * @return StatisticPtr
     */
    static StatisticPtr createInit(const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   uint64_t width,
                                   uint64_t depth,
                                   uint64_t numberOfBitsInKey);

    StatisticValue<> getStatisticValue(const ProbeExpression& probeExpression) const override;
    bool equal(const Statistic& other) const override;
    std::string toString() const override;
    void merge(const SynopsesStatistic& other) override;

    /**
     * @brief Increments the counter at the position of <row, col>
     * @param row
     * @param col
     */
    void update(uint64_t row, uint64_t col);

    uint64_t getWidth() const;
    uint64_t getDepth() const;
    uint64_t getNumberOfBitsInKeyOffset() const;
    std::string getCountMinDataAsString() const;

  private:
    CountMinStatistic(const Windowing::TimeMeasure& startTs,
                      const Windowing::TimeMeasure& endTs,
                      uint64_t observedTuples,
                      uint64_t width,
                      uint64_t depth,
                      uint64_t numberOfBitsInKey,
                      const std::vector<uint64_t>& countMinData);


    uint64_t width;
    uint64_t depth;
    uint64_t numberOfBitsInKey;
    std::vector<uint64_t> countMinData;
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_COUNTMINSTATISTIC_HPP_
