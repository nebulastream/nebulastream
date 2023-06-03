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
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>

namespace NES::Benchmark::DataGeneration::NEXMarkGeneration {

/**
 * @brief struct of a PersonRecord
 */
typedef struct PersonRecordStruct {
    Runtime::TupleBuffer::NestedTupleBufferKey name;
    Runtime::TupleBuffer::NestedTupleBufferKey email;
    Runtime::TupleBuffer::NestedTupleBufferKey phone;
    Runtime::TupleBuffer::NestedTupleBufferKey street;
    Runtime::TupleBuffer::NestedTupleBufferKey city;
    Runtime::TupleBuffer::NestedTupleBufferKey country;
    Runtime::TupleBuffer::NestedTupleBufferKey province;
    uint32_t zipcode;
    Runtime::TupleBuffer::NestedTupleBufferKey homepage;
    Runtime::TupleBuffer::NestedTupleBufferKey creditcard;
    double income;
    Runtime::TupleBuffer::NestedTupleBufferKey interest;
    Runtime::TupleBuffer::NestedTupleBufferKey education;
    Runtime::TupleBuffer::NestedTupleBufferKey gender;
    bool business;
    uint8_t age;
    uint64_t timestamp;
} PersonRecord;

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
};
} //namespace NES::Benchmark::DataGeneration::NEXMarkGeneration

#endif//NES_PERSONGENERATOR_HPP
