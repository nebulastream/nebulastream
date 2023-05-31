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

#include "Util/ZipfianGenerator.hpp"
#include <API/Schema.hpp>
#include <DataGeneration/ByteDataGenerator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <random>
#include <utility>

// for debugging purposes
void printMap(std::string_view comment, const std::map<std::uint8_t, size_t>& m) {
    //src: https://en.cppreference.com/w/cpp/container/map
    std::cout << comment;
    for (const auto& [key, value] : m)
        std::cout << '[' << key << "] = " << value << "; ";
    std::cout << '\n';
}

namespace NES::Benchmark::DataGeneration {
// ====================================================================================================
// Data Generator
// ====================================================================================================

ByteDataGenerator::ByteDataGenerator(Schema::MemoryLayoutType layoutType,
                                     size_t numColumns,
                                     uint8_t minValue,
                                     uint8_t maxValue,
                                     ByteDataDistribution* dataDistribution)
    : minValue(minValue), maxValue(maxValue), dataDistribution(dataDistribution) {
    this->schema = Schema::create();
    schema->setLayoutType(layoutType);
    for (size_t i = 0; i < numColumns; i++) {
        std::string str = "f_" + std::to_string(i);
        schema->addField(str, BasicType::UINT8);
    }
}

std::string ByteDataGenerator::getName() { return "Bytes"; }

static void randomizeHistogramOrder(std::map<uint8_t, size_t>& histogram, size_t seed) {
    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> randomIndex(0, histogram.size() - 1);
    std::vector<uint8_t> keys;
    keys.reserve(histogram.size());
    for (const auto& [key, _] : histogram) {
        keys.push_back(key);
    }
    for (size_t i = 0; i < histogram.size(); i++) {
        size_t swap1 = keys[randomIndex(gen)];
        size_t swap2 = keys[randomIndex(gen)];
        auto tmp = histogram[swap1];
        histogram[swap1] = histogram[swap2];
        histogram[swap2] = tmp;
    }
}

std::vector<Runtime::TupleBuffer> ByteDataGenerator::createData(size_t numBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numBuffers);
    size_t numberOfTuples = 0;

    switch (dataDistribution->getName()) {
        case DistributionName::REPEATING_VALUES: {
            auto distribution = dynamic_cast<RepeatingValues*>(dataDistribution);
            if (distribution->sort)
                NES_THROW_RUNTIME_ERROR("Sorting this distribution is not allowed.");
            std::mt19937 gen(distribution->seed);
            std::uniform_int_distribution<uint8_t> randomValue(minValue, maxValue);
            auto minDeviation = distribution->numRepeats - distribution->sigma;
            if (minDeviation < 0)
                minDeviation = 0;
            auto maxDeviation = distribution->numRepeats + distribution->sigma;
            std::uniform_int_distribution<> randomRepeat(minDeviation, maxDeviation);
            auto value = randomValue(gen);
            for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
                Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
                auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(getMemoryLayout(bufferSize), bufferRef);
                std::vector repeats(dynamicBuffer.getOffsets().size(), distribution->numRepeats);
                for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                    int curRun = 0;
                    size_t row = 0;
                    while (row < dynamicBuffer.getCapacity()) {
                        while (curRun < repeats[col] && row < dynamicBuffer.getCapacity()) {
                            dynamicBuffer[row][col].write<uint8_t>(value);
                            numberOfTuples++;
                            curRun++;
                            row++;
                        }
                        auto newValue = randomValue(gen);
                        while (newValue == value)
                            newValue = randomValue(gen);
                        value = newValue;
                        bool changeRepeats = (rand() % 100) < (100 * distribution->changeProbability);
                        if (changeRepeats)
                            repeats[col] = randomRepeat(gen);
                        curRun = 0;
                    }
                }
                bufferRef.setNumberOfTuples(numberOfTuples);
                createdBuffers.emplace_back(bufferRef);
                numberOfTuples = 0;
            }
            break;
        }
        case DistributionName::UNIFORM: {
            auto distribution = dynamic_cast<Uniform*>(dataDistribution);
            std::mt19937 gen(distribution->seed);
            std::uniform_int_distribution<uint8_t> randomValue(minValue, maxValue);
            for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
                Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
                auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(getMemoryLayout(bufferSize), bufferRef);
                if (distribution->sort) {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        // count all value occurrences
                        std::map<uint8_t, size_t> histogram;
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++) {
                            histogram[randomValue(gen)]++;
                        }
                        randomizeHistogramOrder(histogram, distribution->seed);
                        // write values to buffer
                        size_t row = 0;
                        for (const auto& [value, count] : histogram) {
                            for (size_t i = 0; i < count; i++) {
                                dynamicBuffer[row][col].write<uint8_t>(value);
                                row++;
                                numberOfTuples++;
                            }
                        }
                    }
                } else {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++)
                            dynamicBuffer[row][col].write<uint8_t>(randomValue(gen));
                        numberOfTuples++;
                    }
                }
                bufferRef.setNumberOfTuples(numberOfTuples);
                createdBuffers.emplace_back(bufferRef);
                numberOfTuples = 0;
            }
            break;
        }
        case DistributionName::BINOMIAL: {
            auto distribution = dynamic_cast<Binomial*>(dataDistribution);
            std::mt19937 gen(distribution->seed);
            std::binomial_distribution<uint8_t> randomValue(maxValue - minValue, distribution->probability);
            for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
                Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
                auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(getMemoryLayout(bufferSize), bufferRef);
                if (distribution->sort) {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        // count all value occurrences
                        std::map<uint8_t, size_t> histogram;
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++) {
                            histogram[randomValue(gen) + minValue]++;
                        }
                        randomizeHistogramOrder(histogram, distribution->seed);
                        // write values to buffer
                        size_t row = 0;
                        for (const auto& [value, count] : histogram) {
                            for (size_t i = 0; i < count; i++) {
                                dynamicBuffer[row][col].write<uint8_t>(value);
                                row++;
                                numberOfTuples++;
                            }
                        }
                    }
                } else {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++)
                            dynamicBuffer[row][col].write<uint8_t>(randomValue(gen) + minValue);
                        numberOfTuples++;
                    }
                }
                bufferRef.setNumberOfTuples(numberOfTuples);
                createdBuffers.emplace_back(bufferRef);
                numberOfTuples = 0;
            }
            break;
        }
        case DistributionName::ZIPF: {
            auto distribution = dynamic_cast<Zipf*>(dataDistribution);
            std::mt19937 gen(distribution->seed);
            ZipfianGenerator randomValue(minValue, maxValue, distribution->alpha);
            for (uint64_t curBuffer = 0; curBuffer < numBuffers; curBuffer++) {
                Runtime::TupleBuffer bufferRef = DataGenerator::allocateBuffer();
                auto dynamicBuffer = Runtime::MemoryLayouts::CompressedDynamicTupleBuffer(getMemoryLayout(bufferSize), bufferRef);
                if (distribution->sort) {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        // count all value occurrences
                        std::map<uint8_t, size_t> histogram;
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++) {
                            histogram[randomValue(gen)]++;
                        }
                        randomizeHistogramOrder(histogram, distribution->seed);
                        // write values to buffer
                        size_t row = 0;
                        for (const auto& [value, count] : histogram) {
                            for (size_t i = 0; i < count; i++) {
                                dynamicBuffer[row][col].write<uint8_t>(value);
                                row++;
                                numberOfTuples++;
                            }
                        }
                    }
                } else {
                    for (size_t col = 0; col < dynamicBuffer.getOffsets().size(); col++) {
                        for (size_t row = 0; row < dynamicBuffer.getCapacity(); row++)
                            dynamicBuffer[row][col].write<uint8_t>(randomValue(gen));
                        numberOfTuples++;
                    }
                }
                bufferRef.setNumberOfTuples(numberOfTuples);
                createdBuffers.emplace_back(bufferRef);
                numberOfTuples = 0;
            }
            break;
        }
    }

    return createdBuffers;
}

std::string ByteDataGenerator::toString() {
    std::ostringstream oss;
    oss << getName();
    return oss.str();
}
DistributionName ByteDataGenerator::getDistributionName() { return dataDistribution->getName(); }
SchemaPtr ByteDataGenerator::getSchema() { return schema; }

// ====================================================================================================
// Distributions
// ====================================================================================================
DistributionName ByteDataDistribution::getName() { return distributionName; }
RepeatingValues::RepeatingValues(int numRepeats, uint8_t sigma, double changeProbability) {
    distributionName = DistributionName::REPEATING_VALUES;
    if (numRepeats < 1)
        NES_THROW_RUNTIME_ERROR("Number of numRepeats must be greater than 1.");// TODO max value
    if (sigma < 0)
        NES_THROW_RUNTIME_ERROR("Sigma must be greater than 0.");
    if (changeProbability < 0 || changeProbability > 1)
        NES_THROW_RUNTIME_ERROR("Change probability must be between 0 and 1.");
    this->numRepeats = numRepeats;
    this->sigma = sigma;
    this->changeProbability = changeProbability;
}

Uniform::Uniform() { distributionName = DistributionName::UNIFORM; }

Binomial::Binomial(double probability) {
    if (probability < 0 || probability > 1)
        NES_THROW_RUNTIME_ERROR("Probability must be between 0 and 1.");
    distributionName = DistributionName::BINOMIAL;
    this->probability = probability;
}

Zipf::Zipf(double alpha) {
    if (alpha < 0 || alpha > 1)
        NES_THROW_RUNTIME_ERROR("Alpha must be between 0 and 1.");
    distributionName = DistributionName::ZIPF;
    this->alpha = alpha;
}

}// namespace NES::Benchmark::DataGeneration