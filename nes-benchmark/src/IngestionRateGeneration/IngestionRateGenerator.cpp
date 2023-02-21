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

#include <IngestionRateGeneration/CustomIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/IngestionRateGenerator.hpp>
#include <IngestionRateGeneration/TrigonometricIngestionRateGenerator.hpp>
#include <IngestionRateGeneration/UniformIngestionRateGenerator.hpp>
#include <Util/BenchmarkUtils.hpp>

namespace NES::Benchmark::IngestionRateGeneration {

IngestionRateGeneratorPtr IngestionRateGenerator::createIngestionRateGenerator(E2EBenchmarkConfigOverAllRuns& configOverAllRuns) {
    auto ingestionRateDistributionStr = configOverAllRuns.ingestionRateDistribution->getValue();
    auto ingestionRateDistribution = getDistributionFromString(ingestionRateDistributionStr);
    auto ingestionRateInBuffers = configOverAllRuns.ingestionRateInBuffers->getValue();
    auto ingestionRateCount = configOverAllRuns.ingestionRateCount->getValue();
    auto numberOfPeriods = configOverAllRuns.numberOfPeriods->getValue();

    if (ingestionRateDistribution == IngestionRateDistribution::UNIFORM) {
        return std::make_unique<UniformIngestionRateGenerator>(ingestionRateInBuffers, ingestionRateCount);
    } else if (ingestionRateDistribution == IngestionRateDistribution::SINUS
               || ingestionRateDistribution == IngestionRateDistribution::COSINUS) {
        return std::make_unique<TrigonometricIngestionRateGenerator>(ingestionRateDistribution,
                                                                     ingestionRateInBuffers,
                                                                     ingestionRateCount,
                                                                     numberOfPeriods);
    } else if (ingestionRateDistribution == IngestionRateDistribution::CUSTOM) {
        auto customValues = NES::Util::splitWithStringDelimiter<uint64_t>(configOverAllRuns.customValues->getValue(), ",");
        return std::make_unique<CustomIngestionRateGenerator>(ingestionRateCount, customValues);
    } else {
        NES_THROW_RUNTIME_ERROR("Ingestion rate distribution not supported");
    }
}

IngestionRateDistribution IngestionRateGenerator::getDistributionFromString(std::string& ingestionRateDistribution) {
    if (ingestionRateDistribution == "UNIFORM" || ingestionRateDistribution == "Uniform"
        || ingestionRateDistribution == "uniform")
        return IngestionRateDistribution::UNIFORM;
    else if (ingestionRateDistribution == "SINUS" || ingestionRateDistribution == "Sinus" || ingestionRateDistribution == "sinus")
        return IngestionRateDistribution::SINUS;
    else if (ingestionRateDistribution == "COSINUS" || ingestionRateDistribution == "Cosinus"
             || ingestionRateDistribution == "cosinus")
        return IngestionRateDistribution::COSINUS;
    else if (ingestionRateDistribution == "CUSTOM" || ingestionRateDistribution == "Custom"
             || ingestionRateDistribution == "custom")
        return IngestionRateDistribution::CUSTOM;
    else
        return IngestionRateDistribution::UNDEFINED;
}
}// namespace NES::Benchmark::IngestionRateGeneration
