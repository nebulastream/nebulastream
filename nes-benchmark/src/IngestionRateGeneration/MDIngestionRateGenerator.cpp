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
MDIngestionRateGenerator::MDIngestionRateGenerator(std::string ingestionRateDistribution, uint64_t ingestionRateCnt)
    : IngestionRateGenerator(), ingestionRateDistribution(getDistributionFromString(ingestionRateDistribution)), ingestionRateCnt(ingestionRateCnt) {}

std::vector<std::uint64_t> MDIngestionRateGenerator::generateIngestionRates() {
    for (uint64_t i = 0; i < ingestionRateCnt; ++i) {
        if (ingestionRateDistribution == M1) {
            predefinedIngestionRates.push_back(m1Values[i % 18]);
        } else if (ingestionRateDistribution == M2) {
            predefinedIngestionRates.push_back(m2Values[i % 18]);
        } else if (ingestionRateDistribution == D1) {
            predefinedIngestionRates.push_back(d1Values[i % 30]);
        } else if (ingestionRateDistribution == D2) {
            predefinedIngestionRates.push_back(d2Values[i % 30]);
        }
    }

    return predefinedIngestionRates;
}
}// namespace NES::Benchmark::IngestionRateGeneration
