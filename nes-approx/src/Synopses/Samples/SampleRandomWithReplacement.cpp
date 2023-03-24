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

#include <Synopses/Samples/SampleRandomWithReplacement.hpp>
#include <random>

namespace NES::ASP{

void SampleRandomWithReplacement::addToSynopsis(Nautilus::Record record) {
    // Here we just store all records and then perform the sampling in getApproximate()
    storedRecords.emplace_back(record);
}

Nautilus::Record SampleRandomWithReplacement::getApproximate() {
    // First, we have to pick our sample
    std::vector<Nautilus::Record> sample;
    std::mt19937 generator(GENERATOR_SEED_DEFAULT);
    std::uniform_int_distribution<uint64_t> uniformIntDistribution(0, storedRecords.size());
    while (sample.size() < sampleSize) {
        uint64_t pos = uniformIntDistribution(generator);
        sample.emplace_back(storedRecords[pos]);
    }

    // Approximate over the sample and write the approximation into record
    Nautilus::Record record;
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());
    aggregationFunction->reset(aggregationValueMemRef);
    for (auto& item : sample) {
        aggregationFunction->lift(aggregationValueMemRef, item.read(fieldName));
    }

    auto approximatedValue = aggregationFunction->lower(aggregationValueMemRef);
    record.write(fieldName, approximatedValue);

    return record;
}

SynopsesArguments SRSWR(size_t sampleSize, std::string fieldName,
                        Runtime::Execution::Aggregation::AggregationFunctionPtr aggregationFunction) {
    return SynopsesArguments::createArguments(aggregationFunction, fieldName, SynopsesArguments::Type::SRSWR, sampleSize);
}

} // namespace NES::ASP