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

#include <DataGeneration/ByteDataGenerator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <utility>

namespace NES::Benchmark::DataGeneration {
// ====================================================================================================
// Distributions
// ====================================================================================================
ByteDataDistribution::~ByteDataDistribution() = default;
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
RepeatingValues::~RepeatingValues() {
    numRepeats = 0;
    sigma = 0;
    changeProbability = 0;
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