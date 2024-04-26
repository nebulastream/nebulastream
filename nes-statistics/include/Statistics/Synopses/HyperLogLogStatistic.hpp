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

#ifndef NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_HYPERLOGLOGSTATISTIC_HPP_
#define NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_HYPERLOGLOGSTATISTIC_HPP_

#include <Statistics/Synopses/SynopsesStatistic.hpp>
#include <cmath>
#include <optional>

#if defined(__has_builtin) && (defined(__GNUC__) || defined(__clang__))
#define _GET_CLZ(x, b) (uint8_t)(std::min(b, (uint64_t)::__builtin_clz(x)) + 1)
#else
inline uint8_t _get_leading_zero_count(uint32_t x, uint8_t b) {
#if defined (_MSC_VER)
    uint32_t leading_zero_len = 32;
    ::_BitScanReverse(&leading_zero_len, x);
    --leading_zero_len;
    return std::min(b, (uint8_t)leading_zero_len);
#else
    uint8_t v = 1;
    while (v <= b && !(x & 0x80000000)) {
        v++;
        x <<= 1;
    }
    return v;
#endif
}
#define _GET_CLZ(x, b) _get_leading_zero_count(x, b)
#endif /* defined(__GNUC__) */


namespace NES::Statistic {

/**
 * @brief This class represents a hyper log log statistic, i.e., https://en.wikipedia.org/wiki/HyperLogLog.
 * We choose an implementation by Hideaki Ohno (https://github.com/hideo55/cpp-HyperLogLog)
 * Copyright (c) 2013 Hideaki Ohno <hide.o.j55{at}gmail.com>
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the 'Software'), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
 * to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 */
class HyperLogLogStatistic : public SynopsesStatistic {
  public:
    static StatisticPtr create(const Windowing::TimeMeasure& startTs,
                               const Windowing::TimeMeasure& endTs,
                               uint64_t observedTuples,
                               uint64_t bitWidth,
                               const std::string_view hyperLogLogDataString,
                               double estimate);

    static StatisticPtr createInit(const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   uint64_t bitWidth);

    StatisticValue<> getStatisticValue(const ProbeExpression& probeExpression) const override;
    bool equal(const Statistic& other) const override;
    std::string toString() const override;
    void merge(const SynopsesStatistic& other) override;
    uint64_t getWidth() const;
    double getEstimate() const;
    std::string getHyperLogLogDataAsString() const;

    /**
     * @brief Updates the HyperLogLog sketch with the given hash
     * @param hash
     */
    void update(uint64_t hash);

    /**
     * @brief Performs the estimation of the HyperLogLog sketch and updates the estimate field
     */
    void performEstimation();

  private:
    HyperLogLogStatistic(const Windowing::TimeMeasure& startTs,
                         const Windowing::TimeMeasure& endTs,
                         uint64_t observedTuples,
                         uint64_t bitWidth,
                         const std::vector<uint8_t>& registers,
                         double estimate = std::numeric_limits<double>::quiet_NaN());

    uint64_t bitWidth;
    double_t estimate;
    double_t alphaMM;
    uint64_t registerSize;
    std::vector<uint8_t> registers;
};

}// namespace NES::Statistic

#endif // NES_STATISTICS_INCLUDE_STATISTICS_SYNOPSES_HYPERLOGLOGSTATISTIC_HPP_
