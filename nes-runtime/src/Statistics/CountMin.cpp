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

#include <Statistics/CountMin.hpp>
#include <Statistics/SynopsisProbeParameter/CountMinProbeParameter.hpp>
#include <Statistics/SynopsisProbeParameter/StatisticQueryType.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <random>
#include <vector>

namespace NES::Experimental::Statistics {

CountMin::CountMin(uint64_t width,
                   const std::vector<uint64_t>& data,
                   StatisticCollectorIdentifierPtr statisticCollectorIdentifier,
                   const uint64_t observedTuples,
                   const uint64_t depth)
    : Statistic(std::move(statisticCollectorIdentifier), observedTuples, depth), width(width),
      error(calcError(width)), probability(calcProbability(depth)), data(data), seeds(createSeeds(depth)) {}

double CountMin::calcError(const uint64_t width) { return 1.0 / (double) width; }

double CountMin::calcProbability(const uint64_t depth) { return exp(-1.0 * (double) depth); }

std::vector<uint64_t> CountMin::createSeeds(uint64_t depth, uint64_t seed) {
    std::random_device rd;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint64_t> distribution;

    uint64_t keySizeInBits = 64;

    std::vector<uint64_t> seeds(depth * keySizeInBits, 0);
    for (auto row = 0UL; row < depth; ++row) {
        for (auto keyBit = 0UL; keyBit < keySizeInBits; ++keyBit) {
            seeds[row * keySizeInBits + keyBit] = distribution(gen);
        }
    }
    return seeds;
}

uint64_t CountMin::H3Hash(uint64_t key, uint64_t row) {
    uint64_t hash = 0;
    bool bitSet;
    for (int bitIndex = 0; bitIndex < 64; ++bitIndex) {
        bitSet = key & 1UL;
        if (bitSet) {
            hash ^= seeds[row * width + bitIndex];
        }
    }
    return hash;
}

double CountMin::pointQuery(uint64_t key) {
    std::vector<uint64_t> entries;
    for (auto rowIdx = 0UL; rowIdx < getDepth(); ++rowIdx) {
        auto hash = H3Hash(key, rowIdx) % width;
        entries.push_back(data[rowIdx * width + hash]);
    }
    return static_cast<double>(*std::min_element(entries.begin(), entries.end()));
}

double CountMin::rangeQuery(uint64_t key) {
    double sum = 0;
    for (auto keyIndex = 0UL; keyIndex < key; ++keyIndex) {
        sum += pointQuery(key);
    }
    return sum;
}

double CountMin::probe(NES::Experimental::Statistics::StatisticProbeParameterPtr& probeParameters) {
    auto cmProbeParameters = std::dynamic_pointer_cast<CountMinProbeParameter>(probeParameters);
    auto key = cmProbeParameters->getConstQueryValue();
    auto queryType = cmProbeParameters->getQueryType();
    if (queryType == StatisticQueryType::POINT_QUERY) {
        auto result = pointQuery(key) / getObservedTuples();
        return result;
    } else if (queryType == StatisticQueryType::RANGE_QUERY) {
        auto result = rangeQuery(key) / getObservedTuples();
        if (cmProbeParameters->isInverse()) {
            result = 1.0 - result;
        }
        return result;
    } else {
        NES_ERROR("Invalid Query Type for Count-Min")
    }
    return -1.0;
}

const std::vector<uint64_t>& CountMin::getData() const { return data; }

uint64_t CountMin::getWidth() const { return width; }

double CountMin::getError() const { return error; }

double CountMin::getProbability() const { return probability; }

CountMin CountMin::createFromString(void* cmString, uint64_t depth, uint64_t width) {
    auto* rawData = static_cast<uint64_t*>(cmString);
    std::vector<uint64_t> cmData(rawData, rawData + depth * width);

    return {width, cmData, nullptr, 0, depth};
}

void CountMin::incrementCounter(uint64_t* data, uint64_t rowId, uint64_t width, uint64_t columnId) {
    data[rowId * width + columnId] += 1;
}
}// namespace NES::Experimental::Statistics
