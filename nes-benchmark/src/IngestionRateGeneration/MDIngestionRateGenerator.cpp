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

#include <IngestionRateGeneration/MDIngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
MDIngestionRateGenerator::MDIngestionRateGenerator(IngestionRateDistribution ingestionRateDistribution, uint64_t ingestionRateCount)
    : IngestionRateGenerator(), ingestionRateDistribution(ingestionRateDistribution) {

    IngestionRateGenerator::ingestionRateCount = ingestionRateCount;
}

std::vector<std::uint64_t> MDIngestionRateGenerator::generateIngestionRates() {
    for (uint64_t i = 0; i < ingestionRateCount; ++i) {
        if (ingestionRateDistribution == IngestionRateDistribution::M1) {
            predefinedIngestionRates.push_back(m1Values[i % (sizeof(m1Values) / sizeof(m1Values[0]))]);
        } else if (ingestionRateDistribution == IngestionRateDistribution::M2) {
            predefinedIngestionRates.push_back(m2Values[i % (sizeof(m2Values) / sizeof(m2Values[0]))]);
        } else if (ingestionRateDistribution == IngestionRateDistribution::D1) {
            predefinedIngestionRates.push_back(d1Values[i % (sizeof(d1Values) / sizeof(d1Values[0]))]);
        } else if (ingestionRateDistribution == IngestionRateDistribution::D2) {
            predefinedIngestionRates.push_back(d2Values[i % (sizeof(d2Values) / sizeof(d2Values[0]))]);
        }
    }

    return predefinedIngestionRates;
}
}// namespace NES::Benchmark::IngestionRateGeneration
