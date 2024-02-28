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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <FileDataGenerator.h>
#include <Runtime/MemoryLayout/RowLayout.hpp>

FileDataGenerator::FileDataGenerator(NES::SchemaPtr schema, boost::filesystem::path path)
    : schema(std::move(schema)), path(std::move(path)) {
    if (path.extension() == ".csv") {
        std::vector<NES::PhysicalTypePtr> physicalTypes;
        NES::DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = NES::DefaultPhysicalTypeFactory();
        for (const NES::AttributeFieldPtr& field : schema->fields) {
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            physicalTypes.push_back(physicalField);
        }
        parser_impl = std::make_unique<NES::CSVParser>(schema->fields.size(), physicalTypes, ",");
    } else if (path.extension() == ".json") {
        std::vector<NES::PhysicalTypePtr> physicalTypes;
        std::vector<std::string> schemaKeys;
        NES::DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = NES::DefaultPhysicalTypeFactory();
        for (const NES::AttributeFieldPtr& field : schema->fields) {
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            physicalTypes.push_back(physicalField);
            schemaKeys.push_back(field->getName());
        }
        parser_impl = std::make_unique<NES::JSONParser>(schema->fields.size(), schemaKeys, physicalTypes);
    } else if (path.extension() == ".bin") {
        parser_impl = nullptr;
    } else {
        NES_THROW_RUNTIME_ERROR(fmt::format("File Extension '{}' is not implemented", path.extension().string()));
    }
}
std::vector<NES::Runtime::TupleBuffer> FileDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    if (!file) {
        file.open(path);
    }
    NES_ASSERT(file.good(), "Could not open file");

    if (file.eof()) {
        return {};
    }
    std::vector<NES::Runtime::TupleBuffer> buffers;
    for (size_t current_buffer_index = 0; current_buffer_index < numberOfBuffers; current_buffer_index++) {
        auto buffer = allocateBuffer();
        if (!parser_impl) {
            uint64_t numberOfTuplesInBuffer = 0;
            file.read(reinterpret_cast<char*>(numberOfTuplesInBuffer), sizeof(numberOfTuplesInBuffer));
            NES_ASSERT(file.good(), "Could not read tuplecount from file");
            NES_ASSERT(numberOfTuplesInBuffer * schema->getSchemaSizeInBytes() <= bufferSize, "Buffer to small to load");
            file.read(reinterpret_cast<char*>(buffer.getBuffer()), numberOfTuplesInBuffer * schema->getSchemaSizeInBytes());
            buffer.setNumberOfTuples(numberOfTuplesInBuffer);
            NES_ASSERT(file.good(), "Could not read buffer content");
        } else {
            size_t number_of_tuples_in_buffer = bufferSize / schema->getSchemaSizeInBytes();
            auto dynamicBuffer = NES::Runtime::MemoryLayouts::DynamicTupleBuffer(
                std::make_shared<NES::Runtime::MemoryLayouts::RowLayout>(schema, bufferSize),
                buffer);
            for (size_t current_tuple_index = 0; current_tuple_index < number_of_tuples_in_buffer; current_tuple_index++) {
                if (file.eof()) {
                    break;
                }
                char line_buffer[1024];
                file.getline(line_buffer, 1024);
                NES_ASSERT(file.good(), "Could not read buffer content");
                parser_impl->writeInputTupleToTupleBuffer(std::string(line_buffer, std::strlen(line_buffer)),
                                                          current_tuple_index,
                                                          dynamicBuffer,
                                                          schema,
                                                          bufferManager);
                buffer.setNumberOfTuples(current_buffer_index + 1);
            }
        }
        buffers.emplace_back(std::move(buffer));
    }

    return buffers;
}
NES::SchemaPtr FileDataGenerator::getSchema() { return schema; }
std::string FileDataGenerator::getName() { return fmt::format("FileDataGenerator({})", path.string()); }
std::string FileDataGenerator::toString() { return fmt::format("FileDataGenerator({})", path.string()); }
NES::Configurations::SchemaTypePtr FileDataGenerator::getSchemaType() { NES_NOT_IMPLEMENTED(); }