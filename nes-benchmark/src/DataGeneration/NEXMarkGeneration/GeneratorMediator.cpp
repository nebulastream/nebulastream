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

#include <DataGeneration/NEXMarkGeneration/GeneratorMediator.hpp>
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>
#include <API/Schema.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

GeneratorMediator::GeneratorMediator(size_t numberOfBuffers, size_t bufferSize) {
    auto openAuctionSchema = OpenAuctionGenerator().getSchema();
    auto openAuctionSchemaSize = openAuctionSchema->getSchemaSizeInBytes();
    uint64_t numOpenAuctions = numberOfBuffers * bufferSize / openAuctionSchemaSize;
    uint64_t numPersons = numOpenAuctions / 3;

    auto timeInSec = 0UL;

    for (uint64_t i = 0; i < numOpenAuctions; ++i) {
        std::tuple<uint64_t, uint64_t, uint64_t> auctionRecord;
        auctions.emplace_back(auctionRecord);
    }

    for (uint64_t i = 0; i < numPersons; ++i) {
        std::tuple<uint64_t, uint64_t> personRecord;
        persons.emplace_back(personRecord);
    }
}

GeneratorMediator& GeneratorMediator::getInstance(size_t numberOfBuffers, size_t bufferSize) {
    // C++11 guarantees that static local variables are initialized in a thread-safe manner
    static GeneratorMediator instance(numberOfBuffers, bufferSize);
    return instance;
}
} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
