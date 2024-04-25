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

#include <Statistics/Synopses/HyperLogLogStatistic.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>

namespace NES::Statistic {

StatisticPtr HyperLogLogStatistic::create(const Windowing::TimeMeasure& startTs,
                                          const Windowing::TimeMeasure& endTs,
                                          uint64_t observedTuples,
                                          uint64_t bitWidth,
                                          const std::string_view hyperLogLogDataString,
                                          double estimate) {
    // We just store the HyperLogLog data as a string. So we have to copy the bytes from the string to a vector
    // We do this by first creating a large enough vector and then copy the bytes
    std::vector<uint8_t> hyperLogLogData((1 << bitWidth), 0);
    std::memcpy(hyperLogLogData.data(), hyperLogLogDataString.data(), sizeof(uint8_t) * hyperLogLogData.size());

    return std::make_shared<HyperLogLogStatistic>(HyperLogLogStatistic(startTs, endTs, observedTuples, bitWidth, hyperLogLogData, estimate));
}

StatisticPtr HyperLogLogStatistic::createInit(const Windowing::TimeMeasure& startTs,
                                              const Windowing::TimeMeasure& endTs,
                                              uint64_t bitWidth) {
    std::vector<uint8_t> hyperLogLogData((1 << bitWidth), 0);
    constexpr auto observedTuples = 0;
    return std::make_shared<HyperLogLogStatistic>(HyperLogLogStatistic(startTs, endTs, observedTuples, bitWidth, hyperLogLogData, std::numeric_limits<double>::quiet_NaN()));
}

HyperLogLogStatistic::HyperLogLogStatistic(const Windowing::TimeMeasure& startTs,
                                           const Windowing::TimeMeasure& endTs,
                                           uint64_t observedTuples,
                                           uint64_t bitWidth,
                                           const std::vector<uint8_t>& registers,
                                           double estimate)
: SynopsesStatistic(startTs, endTs, observedTuples), bitWidth(bitWidth), estimate(estimate),
      registerSize(1 << bitWidth), registers(registers) {
    if (bitWidth < 4 || 30 < bitWidth) {
        NES_THROW_RUNTIME_ERROR("bitWidth must be in the range of [4,30]");
    }

    double alpha;
    switch (registerSize) {
        case 16: alpha = 0.673; break;
        case 32: alpha = 0.697; break;
        case 64: alpha = 0.709; break;
        default: alpha = 0.7213 / (1.0 + 1.079 / registerSize); break;
    }
    alphaMM = alpha * registerSize * registerSize;
}

StatisticValue<> HyperLogLogStatistic::getStatisticValue(const ProbeExpression&) const {
    // If the estimate is not calculated yet, we throw an exception
#ifdef __APPLE__
    if (std::isnan(estimate)) {
        NES_THROW_RUNTIME_ERROR("HyperLogLogStatistic has no estimate value. Please call performEstimation() first.");
    }
#else
    if (isnanl(estimate)) {
        NES_THROW_RUNTIME_ERROR("HyperLogLogStatistic has no estimate value. Please call performEstimation() first.");
    }
#endif
    return StatisticValue<>(estimate, startTs, endTs);
}

bool HyperLogLogStatistic::equal(const Statistic& other) const {
    if (other.instanceOf<HyperLogLogStatistic>()) {
        auto otherHyperLogLogStatistic = other.as<const HyperLogLogStatistic>();
        return startTs.equals(otherHyperLogLogStatistic->startTs) && endTs.equals(otherHyperLogLogStatistic->endTs) &&
            observedTuples == otherHyperLogLogStatistic->observedTuples && bitWidth == otherHyperLogLogStatistic->bitWidth &&
            alphaMM == otherHyperLogLogStatistic->alphaMM && registerSize == otherHyperLogLogStatistic->registerSize &&
            estimate == otherHyperLogLogStatistic->estimate &&
            registers == otherHyperLogLogStatistic->registers;
    }
    return false;
}

std::string HyperLogLogStatistic::toString() const {
    std::ostringstream oss;
    oss << "HyperLogLog(";
    oss << "startTs: " << startTs.toString() << " ";
    oss << "endTs: " << endTs.toString() << " ";
    oss << "observedTuples: " << observedTuples << " ";
    oss << "bitWidth: " << bitWidth << " ";
    oss << "estimate: " << estimate << " ";
    oss << "alphaMM: " << alphaMM << " ";
    oss << "registerSize: " << registerSize << " ";
    oss << ")";
    return oss.str();
}

void HyperLogLogStatistic::merge(const SynopsesStatistic& other) {
    // 1. We can only merge the same synopsis type
    if (!other.instanceOf<HyperLogLogStatistic>()) {
        NES_ERROR("Other is not of type HyperLogLogStatistic. Therefore, we skipped merging them together");
        return;
    }

    // 2. We can only merge if the bit bitWidth and the register size are the same
    auto otherHyperLogLog = other.as<const HyperLogLogStatistic>();
    if (bitWidth != otherHyperLogLog->bitWidth || registerSize != otherHyperLogLog->registerSize) {
        NES_ERROR("Bit bitWidth or register size is different. Therefore, we skipped merging them together");
        return;
    }

    // 3. We merge the registers by getting the maximum value of the two registers
    for (auto i = 0_u64; i < registers.size(); ++i) {
        registers[i] = std::max(registers[i], otherHyperLogLog->registers[i]);
    }

    // 4. We add the observed tuples together
    observedTuples += otherHyperLogLog->observedTuples;

    // 5. Calculating the new estimate, as we have changed the registers
    performEstimation();
}

void HyperLogLogStatistic::update(uint64_t hash) {
    // Incrementing the observed tuples as we have seen one more
    observedTuples += 1;

    // Rank is the position of the leftmost 1-bit in the binary representation of the hash
    const auto index = hash >> (64 - bitWidth);
    const auto rank = _GET_CLZ((hash << bitWidth), 64_u64 - bitWidth);
    registers[index] = std::max(registers[index], rank);

    // For now, we calculate for every tuple the new estimate
    performEstimation();
}

void HyperLogLogStatistic::performEstimation() {
    estimate = 0;
    double_t z_sum = 0;
    for (const auto& reg : registers) {
        z_sum += 1.0 / (1 << reg);
    }

    // E in the original paper
    estimate = (alphaMM / z_sum);
    if (estimate < 2.5 * registerSize) {
        auto zeros = std::count(registers.begin(), registers.end(), 0);
        if (zeros != 0) {
            estimate = registerSize * std::log(static_cast<double>(registerSize) / zeros);
        }
    } else if (estimate > (1.0 / 30.0) * std::pow(2, 32)) {
        estimate = -std::pow(2, 32) * std::log(1 - (estimate / std::pow(2, 32)));
    }
}

uint64_t HyperLogLogStatistic::getWidth() const { return bitWidth; }

double HyperLogLogStatistic::getEstimate() const { return estimate; }

std::string HyperLogLogStatistic::getHyperLogLogDataAsString() const {
    const auto dataSizeBytes = registers.size() * sizeof(uint8_t);
    std::string hyperLogLogStr;
    hyperLogLogStr.resize(dataSizeBytes);
    std::memcpy(hyperLogLogStr.data(), registers.data(), dataSizeBytes);
    return hyperLogLogStr;
}

}// namespace NES::Statistic