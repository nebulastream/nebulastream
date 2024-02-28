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

#ifndef NES_DATAGENERATOR_HPP
#define NES_DATAGENERATOR_HPP

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/yaml/Yaml.hpp>
#include <vector>

namespace NES::Configurations {

class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;

}// namespace NES::Configurations

namespace NES::Benchmark::DataGeneration {

class DataGenerator;
using DataGeneratorPtr = std::unique_ptr<DataGenerator>;

class DataGenerator {
  public:
    /**
     * @brief constructor for a data generator
     */
    explicit DataGenerator() = default;

    /**
     * @brief destructor for a data generator
     */
    virtual ~DataGenerator() = default;

    /**
     * @brief creates the data that will be used by a DataProvider
     * @param numberOfBuffers
     * @param bufferSize
     * @return
     */
    virtual std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) = 0;

    /**
     * @brief returns the schema that belongs to this data generation
     * @return schema
     */
    virtual SchemaPtr getSchema() = 0;

    /**
     * @brief overrides the schema from the abstract parent class
     * @return schema from a DefaultDataGenerator
     */
    virtual Configurations::SchemaTypePtr getSchemaType() = 0;

    /**
     * @brief returns the name of the data generator
     * @return name of the data generator
     */
    virtual std::string getName() = 0;

    /**
     * @brief creates a string representation of this data generator
     * @return the string representation
     */
    virtual std::string toString() = 0;

    /**
     * @brief adds a bufferManager to this dataGenerator
     * @param bufferManager
     */
    void setBufferManager(Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief updates timestamps on the buffers
     * @param buffers list of buffers created by the generator
     * @param startingTimestamp the initialTimestamp
     * @param stepSize increment of the timestamp
     * @param stepChanceInPercent the chance that (a full) step happens
     * @param allowPartialSteps
     */
    void insertTimestamps(std::vector<NES::Runtime::TupleBuffer>& buffers,
                          size_t bufferSize,
                          size_t startingTimestamp,
                          size_t stepSize = 1,
                          uint8_t stepChanceInPercent = 100,
                          bool allowPartialSteps = false);

    /**
     * @brief creates a data generator depending on the name
     * @param name
     * @return
     */
    static DataGeneratorPtr createGeneratorByName(std::string name, Yaml::Node generatorNode);

    /**
     * @brief creates either a RowLayout or a ColumnLayout based on the underlying schema
     * @param bufferSize
     * @return RowLayout pointer, ColumnLayout pointer or nullptr
     */
    Runtime::MemoryLayouts::MemoryLayoutPtr getMemoryLayout(size_t bufferSize);

  protected:
    /**
     * @brief allocates a buffer from the bufferManager
     * @return TupleBuffer
     */
    Runtime::TupleBuffer allocateBuffer();
    virtual bool hasTimestampField() {return false;}
    virtual Runtime::MemoryLayouts::DynamicField getTimestampField(Runtime::MemoryLayouts::DynamicTuple /*tuple*/) {
        NES_NOT_IMPLEMENTED();
    }

  protected:
    Runtime::BufferManagerPtr bufferManager;
};
}// namespace NES::Benchmark::DataGeneration

#endif//NES_DATAGENERATOR_HPP
