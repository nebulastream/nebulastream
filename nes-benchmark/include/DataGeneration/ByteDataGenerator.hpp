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

#ifndef NES_BYTEDATAGENERATOR_HPP
#define NES_BYTEDATAGENERATOR_HPP
#include "Runtime/MemoryLayout/CompressedDynamicTupleBuffer.hpp"
#include <DataGeneration/DataGenerator.hpp>
#include <random>

namespace NES::Benchmark::DataGeneration {
enum class DistributionName { REPEATING_VALUES, UNIFORM, BINOMIAL, ZIPF };
[[maybe_unused]] static std::string getDistributionName(enum DistributionName dn) {
    switch (dn) {
        case DistributionName::REPEATING_VALUES: return "Repeating Values";
        case DistributionName::UNIFORM: return "Uniform";
        case DistributionName::BINOMIAL: return "Binomial";
        case DistributionName::ZIPF: return "Zipf";
    }
}

std::random_device rd;

class ByteDataDistribution {
  public:
    virtual DistributionName getName();
    size_t seed = rd();
    bool sort = false;

  protected:
    DistributionName distributionName;
};

class RepeatingValues : public ByteDataDistribution {
  public:
    /**
     * Generate data with repeating values. Each column will has its own distribution.
     * @param numRepeats how often a value shall be repeated consecutively
     * @param sigma maximum deviation of `numRepeats`: [numRepeats - sigma, numRepeats + sigma]
     * @param changeProbability probability that numRepeats will change within [numRepeats - sigma, numRepeats + sigma]
     */
    explicit RepeatingValues(int numRepeats, uint8_t sigma = 0, double changeProbability = 0);
    //DistributionName getName() override;

    int numRepeats;
    uint8_t sigma;
    double changeProbability;
};

class Uniform : public ByteDataDistribution {
  public:
    /**
     * Generate uniformly distributed data
     */
    explicit Uniform();
};

class Binomial : public ByteDataDistribution {
  public:
    explicit Binomial(double probability);

    double probability;
};

class ByteDataGenerator : public DataGenerator {
  public:
    explicit ByteDataGenerator(Schema::MemoryLayoutType layoutType,
                               size_t numColumns,
                               uint8_t minValue,
                               uint8_t maxValue,
                               ByteDataDistribution* dataDistribution);

    std::string getName() override;
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    SchemaPtr getSchema() override;
    DistributionName getDistributionName();
    std::string toString() override;

  private:
    SchemaPtr schema;
    uint8_t minValue;
    uint8_t maxValue;
    ByteDataDistribution* dataDistribution;
};

}// namespace NES::Benchmark::DataGeneration

#endif//NES_BYTEDATAGENERATOR_HPP
