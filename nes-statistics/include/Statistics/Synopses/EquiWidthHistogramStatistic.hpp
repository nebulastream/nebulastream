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

#ifndef NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_EQUIWIDTHHISTOGRAMSTATISTIC_HPP_
#define NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_EQUIWIDTHHISTOGRAMSTATISTIC_HPP_
#include <Statistics/Synopses/SynopsesStatistic.hpp>

namespace NES::Statistic {
/**
 * @brief EquiWidthHistogramStatistic is a statistic that represents an equi-width histogram.
 * The histogram is represented by a vector of bins, where each bin has a lower and upper bound and a count.
 */
class EquiWidthHistogramStatistic : public SynopsesStatistic {
  public:

    struct Bin {
        Bin(int64_t lowerBound, int64_t upperBound, uint64_t count) : lowerBound(lowerBound), upperBound(upperBound), count(count) {}
        Bin() : Bin(0, 0, 0) {}
        bool operator==(const Bin& other) const {
            return lowerBound == other.lowerBound && upperBound == other.upperBound && count == other.count;
        }
        int64_t lowerBound;
        int64_t upperBound;
        uint64_t count;
    };

    static StatisticPtr create(const Windowing::TimeMeasure& startTs,
                               const Windowing::TimeMeasure& endTs,
                               uint64_t observedTuples,
                               uint64_t binWidth,
                               const std::string_view equiWidthDataString,
                               const uint64_t numberOfBins);

    static StatisticPtr createInit(const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   uint64_t binWidth);

    /**
     * @brief Update the bin belonging to lowerBound and upperBound by increment
     * @param lowerBound
     * @param upperBound
     * @param increment
     */
    void update(int64_t lowerBound, int64_t upperBound, uint64_t increment);


    void* getStatisticData() const override;
    bool equal(const Statistic& other) const override;
    std::string toString() const override;
    void merge(const SynopsesStatistic& other) override;
    std::string getEquiWidthHistDataAsString() const;
    uint64_t getNumberOfBins() const;
    uint64_t getBinWidth() const;
    uint64_t getCountForValue(int64_t value) const;


  private:
    EquiWidthHistogramStatistic(const Windowing::TimeMeasure& startTs,
                                const Windowing::TimeMeasure& endTs,
                                const uint64_t observedTuples,
                                const uint64_t binWidth,
                                const std::vector<Bin>& equiWidthData);


    uint64_t binWidth;
    // We might want to have something more efficient than a vector here
    std::vector<Bin> equiWidthData;
};
} // namespace NES::Statistic
#endif//NES_NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_EQUIWIDTHHISTOGRAMSTATISTIC_HPP_
