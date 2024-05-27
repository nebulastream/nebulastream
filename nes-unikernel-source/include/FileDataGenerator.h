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

#ifndef NES_FILEDATAGENERATOR_H
#define NES_FILEDATAGENERATOR_H

#include <API/Schema.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <filesystem>
#include <fstream>

class FileDataGenerator : public NES::Benchmark::DataGeneration::DataGenerator {
    NES::SchemaPtr schema;
    std::optional<size_t> numberOfTupleToCreate;
    std::filesystem::path path;
    std::ifstream file;
    std::unique_ptr<NES::Parser> parser_impl;

  public:
    FileDataGenerator(NES::SchemaPtr schema, std::filesystem::path path);
    std::vector<NES::Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;
    NES::SchemaPtr getSchema() override;
    std::string getName() override;
    std::string toString() override;
    [[nodiscard]] std::optional<size_t> getNumberOfTupleToCreate() const { return numberOfTupleToCreate; }
    void setNumberOfTupleToCreate(const std::optional<size_t>& numberOfTupleToCreate) {
        this->numberOfTupleToCreate = numberOfTupleToCreate;
    }
    static std::unique_ptr<FileDataGenerator> create(NES::SchemaPtr schema, std::filesystem::path path);
    NES::Configurations::SchemaTypePtr getSchemaType() override;
};

#endif//NES_FILEDATAGENERATOR_H
