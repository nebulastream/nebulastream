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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_RESERVOIRSAMPLESTATISTIC_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_RESERVOIRSAMPLESTATISTIC_HPP_

#include <API/Schema.hpp>
#include <Statistics/Synopses/SynopsesStatistic.hpp>
#include <random>

namespace NES::Statistic {

// We need to use a static seed for the random number generator to ensure reproducibility
static constexpr auto RESERVOIR_SAMPLE_SEED = 42;

class ReservoirSampleStatistic : public SynopsesStatistic {
  public:

    /**
     * @brief Factory method for creating a ReservoirSampleStatistic
     * @param startTs
     * @param endTs
     * @param observedTuples
     * @param sampleSize
     * @param reservoirSampleDataString
     * @param memoryLayout
     * @return StatisticPtr
     */
    static StatisticPtr create(const Windowing::TimeMeasure& startTs,
                               const Windowing::TimeMeasure& endTs,
                               const uint64_t observedTuples,
                               const uint64_t sampleSize,
                               const std::string_view reservoirSampleDataString,
                               const SchemaPtr schema);

    /**
     * @brief Factory method for creating a ReservoirSampleStatistic for the given startTs, endTs, and sampleSize.
     * The #observedTuples are set to 0 and the reservoir space is initialized with 0
     * @param startTs
     * @param endTs
     * @param sampleSize
     * @param schema
     * @return StatisticPtr
     */
    static StatisticPtr createInit(const Windowing::TimeMeasure& startTs,
                                   const Windowing::TimeMeasure& endTs,
                                   const uint64_t sampleSize,
                                   const SchemaPtr schema);

    void* getStatisticData() const override;
    bool equal(const Statistic& other) const override;
    std::string toString() const override;
    void merge(const SynopsesStatistic& other) override;
    int8_t* getReservoirSpace() const;
    std::string getReservoirSpaceAsString() const;
    uint64_t getSampleSize() const;
    SchemaPtr getSchema() const;

    /**
     * @brief Returns the next random integer in the range of [0, observedTuples]
     * This has to happen on a per sample basis due to the following reasons:
     *      1. We want to have a deterministic behavior for the reservoir sample. This means that we have to use the same
     *         seed for the random number generator.
     *      2. We want to get different random numbers, thus a generator must have the same lifetime as the reservoir
     * @return uint64_t
     */
    uint64_t getNextRandomInteger();

  private:
    ReservoirSampleStatistic(const Windowing::TimeMeasure& startTs,
                             const Windowing::TimeMeasure& endTs,
                             uint64_t observedTuples,
                             uint64_t sampleSize,
                             const std::vector<int8_t>& reservoirSpace,
                             const SchemaPtr schema);

    uint64_t sampleSize;
    std::vector<int8_t> reservoirSpace;
    std::mt19937_64 gen;
    const uint64_t tupleSize;
    const SchemaPtr schema;
};


}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_STATISTICS_SYNOPSES_RESERVOIRSAMPLESTATISTIC_HPP_
