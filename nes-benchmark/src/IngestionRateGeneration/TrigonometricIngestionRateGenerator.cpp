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

#include <IngestionRateGeneration/TrigonometricIngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {
TrigonometricIngestionRateGenerator::TrigonometricIngestionRateGenerator(IngestionRateDistribution ingestionRateDistribution,
                                                                         uint64_t ingestionRateInBuffers,
                                                                         uint64_t ingestionRateCnt,
                                                                         uint64_t numberOfPeriods)
    : IngestionRateGenerator(), ingestionRateDistribution(ingestionRateDistribution),
      ingestionRateInBuffers(ingestionRateInBuffers), ingestionRateCnt(ingestionRateCnt), numberOfPeriods(numberOfPeriods) {}

std::vector<std::uint64_t> TrigonometricIngestionRateGenerator::generateIngestionRates() {
    for (uint64_t i = 0; i < ingestionRateCnt; ++i) {
        if (ingestionRateDistribution == SINUS) {
            uint64_t curIngestionRate = round(getSinValue(i) * ingestionRateInBuffers);
            predefinedIngestionRates.push_back(curIngestionRate);
        } else if (ingestionRateDistribution == COSINUS) {
            uint64_t curIngestionRate = round(getCosValue(i) * ingestionRateInBuffers);
            predefinedIngestionRates.push_back(curIngestionRate);
        }
    }

    return predefinedIngestionRates;
}

double TrigonometricIngestionRateGenerator::getSinValue(uint64_t x) {
    return (0.5 * (1 + sin(2.0 * std::numbers::pi * x * (numberOfPeriods / (double) ingestionRateCnt))));
}

double TrigonometricIngestionRateGenerator::getCosValue(uint64_t x) {
    return (0.5 * (1 + cos(2.0 * std::numbers::pi * x * (numberOfPeriods / (double) ingestionRateCnt))));
}
}// namespace NES::Benchmark::IngestionRateGeneration
