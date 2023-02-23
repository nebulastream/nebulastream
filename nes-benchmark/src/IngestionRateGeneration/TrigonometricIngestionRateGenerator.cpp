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
                                                                         uint64_t ingestionRateCount,
                                                                         uint64_t numberOfPeriods)
    : IngestionRateGenerator(), ingestionRateDistribution(ingestionRateDistribution),
      ingestionRateInBuffers(ingestionRateInBuffers), numberOfPeriods(numberOfPeriods) {

    IngestionRateGenerator::ingestionRateCount = ingestionRateCount;
}

std::vector<std::uint64_t> TrigonometricIngestionRateGenerator::generateIngestionRates() {
    for (uint64_t i = 0; i < ingestionRateCount; ++i) {
        if (ingestionRateDistribution == IngestionRateDistribution::SINUS) {
            uint64_t curIngestionRate = round(getSinValue(i) * ingestionRateInBuffers);
            predefinedIngestionRates.push_back(curIngestionRate);
        } else if (ingestionRateDistribution == IngestionRateDistribution::COSINUS) {
            uint64_t curIngestionRate = round(getCosValue(i) * ingestionRateInBuffers);
            predefinedIngestionRates.push_back(curIngestionRate);
        }
    }

    return predefinedIngestionRates;
}

double TrigonometricIngestionRateGenerator::getSinValue(uint64_t xValue) {
    return (0.5 * (1 + sin(2.0 * PI() * xValue * (numberOfPeriods / (double) ingestionRateCount))));
}

double TrigonometricIngestionRateGenerator::getCosValue(uint64_t xValue) {
    return (0.5 * (1 + cos(2.0 * PI() * xValue * (numberOfPeriods / (double) ingestionRateCount))));
}
}// namespace NES::Benchmark::IngestionRateGeneration
