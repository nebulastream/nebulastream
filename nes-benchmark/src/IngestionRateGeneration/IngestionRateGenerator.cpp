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

#include <IngestionRateGeneration/IngestionRateGenerator.hpp>
#include <IngestionRateGeneration/TrigonometricIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/MDIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/UniformIngestionRateGenerator.hpp>

namespace NES::Benchmark::IngestionRateGeneration {

IngestionRateGeneratorPtr IngestionRateGenerator::createIngestionRateGenerator(E2EBenchmarkConfigOverAllRuns& configOverAllRuns) {
    auto ingestionRateDistribution = getDistributionFromString(configOverAllRuns.ingestionRateDistribution->getValue());
    auto ingestionRateInBuffers = configOverAllRuns.ingestionRateInBuffers->getValue();
    auto ingestionRateCnt = configOverAllRuns.ingestionRateCnt->getValue();
    auto numberOfPeriods = configOverAllRuns.numberOfPeriods->getValue();

    if (ingestionRateDistribution == UNIFORM) {
        return std::make_unique<UniformIngestionRateGenerator>(ingestionRateInBuffers, ingestionRateCnt);
    } else if (ingestionRateDistribution == SINUS || ingestionRateDistribution == COSINUS) {
        return std::make_unique<TrigonometricIngestionRateGenerator>(ingestionRateDistribution, ingestionRateInBuffers, ingestionRateCnt, numberOfPeriods);
    } else if ( ingestionRateDistribution == M1 || ingestionRateDistribution == M2 ||
                ingestionRateDistribution == D1 || ingestionRateDistribution == D2) {
        return std::make_unique<MDIngestionRateGenerator>(ingestionRateDistribution, ingestionRateCnt);
    } else {
        NES_THROW_RUNTIME_ERROR("Ingestion rate distribution not supported. Could not create IngestionRateGenerator");
    }
}

IngestionRateDistribution IngestionRateGenerator::getDistributionFromString(std::string ingestionRateDistribution) {
    if (ingestionRateDistribution == "UNIFORM" || ingestionRateDistribution == "Uniform" || ingestionRateDistribution == "uniform") return UNIFORM;
    else if (ingestionRateDistribution == "SINUS" || ingestionRateDistribution == "Sinus" || ingestionRateDistribution == "sinus") return SINUS;
    else if (ingestionRateDistribution == "COSINUS" || ingestionRateDistribution == "Cosinus" || ingestionRateDistribution == "cosinus") return COSINUS;
    else if (ingestionRateDistribution == "M1" || ingestionRateDistribution == "m1") return M1;
    else if (ingestionRateDistribution == "M2" || ingestionRateDistribution == "m2") return M2;
    else if (ingestionRateDistribution == "D1" || ingestionRateDistribution == "d1") return D1;
    else if (ingestionRateDistribution == "D2" || ingestionRateDistribution == "d2") return D2;
    else NES_THROW_RUNTIME_ERROR("Ingestion rate distribution not supported");
}
}// namespace NES::Benchmark::IngestionRateGeneration
