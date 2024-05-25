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
#include <API/Schema.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Expressions/ConstantValueExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Statistics/StatisticUtil.hpp>
#include <Statistics/Synopses/ReservoirSampleStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <random>

namespace NES::Statistic {

StatisticPtr ReservoirSampleStatistic::create(const Windowing::TimeMeasure& startTs,
                                              const Windowing::TimeMeasure& endTs,
                                              const uint64_t observedTuples,
                                              const uint64_t sampleSize,
                                              const std::string_view reservoirSampleDataString,
                                              const SchemaPtr schema) {
    // We just store the ReservoirSample data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    const auto dataSize = sampleSize * schema->getSchemaSizeInBytes();
    std::vector<int8_t> reservoirSpace(dataSize, 0);
    std::memcpy(reservoirSpace.data(), reservoirSampleDataString.data(), sizeof(int8_t) * dataSize);
    return std::make_shared<ReservoirSampleStatistic>(ReservoirSampleStatistic(startTs, endTs, observedTuples, sampleSize, reservoirSpace, schema));
}

StatisticPtr ReservoirSampleStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                                  const Windowing::TimeMeasure& endTs,
                                                  const uint64_t sampleSize,
                                                  const SchemaPtr schema) {
    const auto dataSize = sampleSize * schema->getSchemaSizeInBytes();
    std::vector<int8_t> reservoirSpace(dataSize, 0);
    constexpr auto observedTuples = 0;
    return std::make_shared<ReservoirSampleStatistic>(ReservoirSampleStatistic(startTs, endTs, observedTuples, sampleSize, reservoirSpace, schema));
}

std::string ReservoirSampleStatistic::getReservoirSpaceAsString() const {
    return std::string(reservoirSpace.begin(), reservoirSpace.end());
}

uint64_t ReservoirSampleStatistic::getSampleSize() const { return sampleSize; }

uint64_t ReservoirSampleStatistic::getNextRandomInteger() {
    std::uniform_int_distribution<uint64_t> distribution(0_u64, observedTuples);
    return distribution(gen);
}

bool ReservoirSampleStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<ReservoirSampleStatistic>()) {
        auto otherReservoirSample = other.as<const ReservoirSampleStatistic>();
        return startTs.equals(otherReservoirSample->startTs) && endTs.equals(otherReservoirSample->endTs) &&
               observedTuples == otherReservoirSample->observedTuples && sampleSize == otherReservoirSample->sampleSize &&
               reservoirSpace == otherReservoirSample->reservoirSpace;
    }
    return false;
}

std::string ReservoirSampleStatistic::toString() const {
    std::ostringstream oss;
    oss << "ReservoirSample(";
    oss << "startTs: " << startTs.toString() << " ";
    oss << "endTs: " << endTs.toString() << " ";
    oss << "observedTuples: " << observedTuples << " ";
    oss << "sampleSize: " << sampleSize << " ";
    oss << ")";
    return oss.str();
}

void ReservoirSampleStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<ReservoirSampleStatistic>()) {
        NES_ERROR("Other is not of type CountMinStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge statistics with the same period
    if (!startTs.equals(other.getStartTs()) || !endTs.equals(other.getEndTs())) {
        NES_ERROR("Can not merge HyperLogLog statistics with different periods");
        return;
    }


    /* 3. We merge two samples together by taking the union of the two samples and then drawing randomly from the union until
     * we reach the sample size of this object.
     * This might not be the most "accurate" version, so this code might change in the future.
     */
    const auto otherReservoirSample = other.as<const ReservoirSampleStatistic>();
    const auto sampleSizeTogether = sampleSize + otherReservoirSample->sampleSize;
    std::vector<int8_t> newReservoirSpace(sampleSize * tupleSize);

    // Drawing random samples from the union until we reach the sample size
    std::mt19937_64 gen(RESERVOIR_SAMPLE_SEED);
    std::uniform_int_distribution distribution(0_u64, sampleSizeTogether);
    for (auto i = 0_u64; i < sampleSize; ++i) {
        const auto newSampleStartPosition = newReservoirSpace.data() + i * tupleSize;
        const auto randomIndex = distribution(gen);
        if (randomIndex < sampleSize) {
            const auto sampleStartPosition = reservoirSpace.data() + randomIndex * tupleSize;
            std::memcpy(newSampleStartPosition, sampleStartPosition, tupleSize);
        } else {
            // We have to subtract the sample size from the random index, as we are now in the other sample
            const auto sampleStartPosition = otherReservoirSample->reservoirSpace.data() + (randomIndex - sampleSize) * tupleSize;
            std::memcpy(newSampleStartPosition, sampleStartPosition, tupleSize);
        }
    }

    reservoirSpace = newReservoirSpace;
    observedTuples += otherReservoirSample->observedTuples;
}

int8_t* ReservoirSampleStatistic::getReservoirSpace() const {
    // This const cast is fine, as we are passing it to the Nautilus runtime.
    return const_cast<int8_t*>(reservoirSpace.data());
}

SchemaPtr ReservoirSampleStatistic::getSchema() const { return schema; }

void* ReservoirSampleStatistic::getStatisticData() const { return getReservoirSpace(); }

ReservoirSampleStatistic::ReservoirSampleStatistic(const Windowing::TimeMeasure& startTs,
                                                   const Windowing::TimeMeasure& endTs,
                                                   uint64_t observedTuples,
                                                   uint64_t sampleSize,
                                                   const std::vector<int8_t>& reservoirSpace,
                                                   const SchemaPtr schema)
    : SynopsesStatistic(startTs, endTs, observedTuples), sampleSize(sampleSize), reservoirSpace(reservoirSpace),
      gen(RESERVOIR_SAMPLE_SEED), tupleSize(schema->getSchemaSizeInBytes()), schema(std::move(schema)) {}

}// namespace NES::Statistic