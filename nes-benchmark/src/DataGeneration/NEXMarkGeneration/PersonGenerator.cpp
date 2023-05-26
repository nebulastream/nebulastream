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

#include <DataGeneration/NEXMarkGeneration/PersonGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

PersonGenerator::PersonGenerator()
    : DataGenerator() {}

std::vector<Runtime::TupleBuffer> PersonGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::random_device rndDevice;
    std::mt19937 generator(rndDevice());
    std::uniform_int_distribution<uint64_t> uniformBooleanDistribution(0, 1);
    // TODO add other distributions

    auto& dependencyGeneratorInstance = DependencyGenerator::getInstance(numberOfBuffers, bufferSize);
    auto persons = dependencyGeneratorInstance.getPersons();
    auto numberOfPersons = persons.size();
    auto numberOfRecords = dependencyGeneratorInstance.getNumberOfRecords();
    auto personsToProcess = numberOfRecords < numberOfPersons ? numberOfRecords : numberOfPersons;

    std::vector<Runtime::TupleBuffer> createdBuffers;
    uint64_t numberOfBuffersToCreate = 1 + personsToProcess * getSchema()->getSchemaSizeInBytes() / bufferSize;
    createdBuffers.reserve(numberOfBuffersToCreate);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    auto processedPersons = 0UL;

    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffersToCreate; ++curBuffer) {
        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // TODO add designated branch for RowLayout to make it faster (cmp. DefaultDataGenerator.cpp)
        for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity() && processedPersons < personsToProcess; ++curRecord) {
            auto auctionsIndex = processedPersons++;

            // TODO create random data
            // TODO add fields
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    return createdBuffers;
}

SchemaPtr PersonGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("id", BasicType::UINT64))
        ->addField(createField("name", BasicType::TEXT))
        ->addField(createField("email", BasicType::TEXT))
        ->addField(createField("phone", BasicType::TEXT))
        ->addField(createField("street", BasicType::TEXT))
        ->addField(createField("city", BasicType::TEXT))
        ->addField(createField("country", BasicType::TEXT))
        ->addField(createField("province", BasicType::TEXT))
        ->addField(createField("zipcode", BasicType::TEXT))
        ->addField(createField("homepage", BasicType::TEXT))
        ->addField(createField("creditcard", BasicType::TEXT))
        ->addField(createField("income", BasicType::TEXT))
        ->addField(createField("interest", BasicType::TEXT))
        ->addField(createField("education", BasicType::TEXT))
        ->addField(createField("gender", BasicType::TEXT))
        ->addField(createField("business", BasicType::TEXT))
        ->addField(createField("age", BasicType::TEXT));
}

std::string PersonGenerator::getName() { return "NEXMarkPerson"; }

std::string PersonGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << "()";

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration::NEXMarkGeneration
