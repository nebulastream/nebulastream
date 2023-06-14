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

#ifndef NES_PERSONGENERATOR_HPP
#define NES_PERSONGENERATOR_HPP

#include <DataGeneration/DataGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/DependencyGenerator.hpp>
#include <DataGeneration/NEXMarkGeneration/PersonDataPool.hpp>
#include <DataGeneration/NEXMarkGeneration/UniformIntDistributions.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

class PersonGenerator;
using PersonRecord = std::tuple<uint64_t, Runtime::TupleBuffer::NestedTupleBufferKey, Runtime::TupleBuffer::NestedTupleBufferKey,
                                Runtime::TupleBuffer::NestedTupleBufferKey, Runtime::TupleBuffer::NestedTupleBufferKey,
                                Runtime::TupleBuffer::NestedTupleBufferKey, Runtime::TupleBuffer::NestedTupleBufferKey,
                                Runtime::TupleBuffer::NestedTupleBufferKey, uint32_t, Runtime::TupleBuffer::NestedTupleBufferKey,
                                Runtime::TupleBuffer::NestedTupleBufferKey, double, Runtime::TupleBuffer::NestedTupleBufferKey,
                                Runtime::TupleBuffer::NestedTupleBufferKey, Runtime::TupleBuffer::NestedTupleBufferKey,
                                bool, uint8_t, uint64_t>;

class PersonGenerator : public DataGenerator {
  public:
    /**
     * @brief creates data with the schema "id, name, email, phone, street, city, country, province, zipcode, homepage,
     * creditcard, income, interest, education, gender, business, age, timestamp" from the persons vector of
     * dependencyGeneratorInstance. All values except timestamp are drawn randomly from uniform distributions in predefined ranges
     * @param numberOfBuffers
     * @param bufferSize
     * @return the TupleBuffer vector
     */
    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    /**
     * @brief overrides the schema from the abstract parent class
     * @return schema of the PersonGenerator
     */
    SchemaPtr getSchema() override;

    /**
     * @brief overrides the name from the abstract parent class
     * @return name of the PersonGenerator
     */
    std::string getName() override;

    /**
     * @brief overrides the string representation of the parent class
     * @return the string representation of the PersonGenerator
     */
    std::string toString() override;

  private:
    /**
     * @brief generates a PersonRecord
     * @param persons
     * @param personsIndex
     * @param dynamicBuffer
     * @return PersonRecord
     */
    PersonRecord generatePersonRecord(std::vector<uint64_t>& persons, uint64_t personsIndex,
                                      Runtime::MemoryLayouts::DynamicTupleBuffer dynamicBuffer);

    /**
     * @brief generates an address
     * @param uniformIntDistributions
     * @param fields
     * @return zipcode
     */
    uint32_t generatePersonAddress(UniformIntDistributions uniformIntDistributions, std::vector<std::string>& fields);

    /**
     * @brief generates a profile
     * @param uniformIntDistributions
     * @param fields
     * @return income, business, age
     */
    std::tuple<double, bool, uint8_t> generatePersonProfile(UniformIntDistributions uniformIntDistributions, std::vector<std::string>& fields);
};
} //namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_PERSONGENERATOR_HPP
