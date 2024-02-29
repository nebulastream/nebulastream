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

FileDataGenerator::FileDataGenerator(NES::SchemaPtr s, boost::filesystem::path p) : schema(std::move(s)), path(std::move(p)) {
    auto extension = path.extension().string();
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
    if (!file.is_open()) {
        file.open(path);
    }
    if (file.eof())
        return {};

    NES_ASSERT(file.good(), "Could not open file");
    std::vector<NES::Runtime::TupleBuffer> buffers;
    for (size_t current_buffer_index = 0; current_buffer_index < numberOfBuffers; current_buffer_index++) {
        auto buffer = allocateBuffer();
        if (!parser_impl) {
            if (file.eof()) {
                break;
            }
            uint64_t expectedBufferSize = 0;
            file.read(reinterpret_cast<char*>(&expectedBufferSize), sizeof(expectedBufferSize));
            if (expectedBufferSize == 0) {
                NES_ASSERT(file.eof(), "Expected end of file");
                break;
            }

            NES_ASSERT(file.good(), "Could not read tuplecount from file");

            NES_ASSERT(expectedBufferSize <= bufferSize, "Buffer to small to load");
            NES_ASSERT(expectedBufferSize % schema->getSchemaSizeInBytes() == 0, "Buffer schema missmatch");
            file.read(reinterpret_cast<char*>(buffer.getBuffer()), expectedBufferSize);
            buffer.setNumberOfTuples(expectedBufferSize / schema->getSchemaSizeInBytes());
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
                file.getline(line_buffer, 1023);
                if (file.bad())
                    throw file.exceptions();
                if (file.eof())
                    break;
                std::string line(line_buffer, std::strlen(line_buffer));

                parser_impl->writeInputTupleToTupleBuffer(line, current_tuple_index, dynamicBuffer, schema, bufferManager);
                buffer.setNumberOfTuples(current_tuple_index + 1);
            }
        }
        buffers.emplace_back(std::move(buffer));
    }

    return buffers;
}
NES::SchemaPtr FileDataGenerator::getSchema() { return schema; }
std::string FileDataGenerator::getName() { return fmt::format("FileDataGenerator({})", path.string()); }
std::string FileDataGenerator::toString() { return fmt::format("FileDataGenerator({})", path.string()); }
std::unique_ptr<FileDataGenerator> FileDataGenerator::create(NES::SchemaPtr schema, boost::filesystem::path path) {
    return std::make_unique<FileDataGenerator>(schema, path);
}
NES::Configurations::SchemaTypePtr FileDataGenerator::getSchemaType() { NES_NOT_IMPLEMENTED(); }