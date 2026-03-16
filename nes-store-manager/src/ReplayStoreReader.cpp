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

#include <ReplayStoreReader.hpp>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <ios>
#include <iosfwd>
#include <string>
#include <utility>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include "DataTypes/Schema.hpp"
#include "ReplayStoreFormat.hpp"

namespace NES::StoreManager
{

ReplayStoreReader::ReplayStoreReader(std::string filePath) : filePath(std::move(filePath))
{
}

ReplayStoreReader::~ReplayStoreReader()
{
    if (inputFile.is_open())
    {
        inputFile.close();
    }
}

void ReplayStoreReader::open()
{
    NES_DEBUG("ReplayStoreReader: opening {}", filePath);
    inputFile = std::ifstream(filePath, std::ios::binary);
    if (!inputFile)
    {
        throw CannotOpenSink("Could not open input file: {}: {}", filePath, std::strerror(errno));
    }
    auto [parsedHeader, offset] = parseHeader(inputFile);
    header = std::move(parsedHeader);
    dataStartOffset = offset;
    opened = true;
    NES_DEBUG("ReplayStoreReader: dataStartOffset={}", dataStartOffset);
}

void ReplayStoreReader::close()
{
    inputFile.close();
    opened = false;
}

Schema ReplayStoreReader::readSchema() const
{
    if (!opened)
    {
        throw CannotOpenSink("ReplayStoreReader: file not opened");
    }
    return parseSchemaFromText(header.schemaText);
}

void ReplayStoreReader::verifySchema(const Schema& expectedSchema) const
{
    auto fileSchema = readSchema();

    if (fileSchema.getNumberOfFields() != expectedSchema.getNumberOfFields())
    {
        throw InvalidConfigParameter(
            "ReplayStoreReader: schema field count mismatch: file has {} fields, expected {}",
            fileSchema.getNumberOfFields(),
            expectedSchema.getNumberOfFields());
    }

    for (size_t i = 0; i < fileSchema.getNumberOfFields(); ++i)
    {
        const auto& fileField = fileSchema.getFieldAt(i);
        const auto& expectedField = expectedSchema.getFieldAt(i);

        // Compare unqualified field names
        auto fileName = fileField.name;
        if (const auto pos = fileName.rfind('$'); pos != std::string::npos)
        {
            fileName = fileName.substr(pos + 1);
        }
        auto expectedName = expectedField.name;
        if (const auto pos = expectedName.rfind('$'); pos != std::string::npos)
        {
            expectedName = expectedName.substr(pos + 1);
        }

        if (fileName != expectedName)
        {
            throw InvalidConfigParameter(
                "ReplayStoreReader: field name mismatch at index {}: file has '{}', expected '{}'", i, fileName, expectedName);
        }
        if (fileField.dataType != expectedField.dataType)
        {
            throw InvalidConfigParameter(
                "ReplayStoreReader: field type mismatch at index {} ({}): file has {}, expected {}",
                i,
                fileName,
                fileField.dataType,
                expectedField.dataType);
        }
    }
}

uint64_t ReplayStoreReader::readRows(char* dest, uint64_t maxRows, uint32_t tupleSize, const Schema& schema)
{
    // Calculate field layout: offsets and sizes within each row as stored in the binary file
    std::vector<uint64_t> fieldOffsets;
    std::vector<uint64_t> fieldSizes;
    uint64_t currentOffset = 0;

    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        fieldOffsets.push_back(currentOffset);
        auto type = schema.getFieldAt(i).dataType;
        uint64_t size = 0;
        if (type.isType(DataType::Type::VARSIZED))
        {
            size = sizeof(uint32_t);
        }
        else
        {
            size = type.getSizeInBytes();
        }
        fieldSizes.push_back(size);
        currentOffset += size;
    }

    uint64_t tuplesRead = 0;
    for (; tuplesRead < maxRows; ++tuplesRead)
    {
        bool rowOk = true;
        for (size_t fieldIdx = 0; fieldIdx < schema.getNumberOfFields(); ++fieldIdx)
        {
            auto type = schema.getFieldAt(fieldIdx).dataType;
            const auto fieldSize = fieldSizes[fieldIdx];
            const auto offset = (tuplesRead * tupleSize) + fieldOffsets[fieldIdx];
            char* fieldDest = dest + offset;

            if (type.isType(DataType::Type::VARSIZED))
            {
                uint32_t len = 0;
                inputFile.read(reinterpret_cast<char*>(&len), sizeof(uint32_t));
                if (!inputFile)
                {
                    rowOk = false;
                    break;
                }
                inputFile.seekg(len, std::ios::cur);
                if (!inputFile)
                {
                    rowOk = false;
                    break;
                }
                std::memset(fieldDest, 0, fieldSize);
            }
            else
            {
                inputFile.read(fieldDest, static_cast<std::streamsize>(fieldSize));
                if (!inputFile)
                {
                    rowOk = false;
                    break;
                }
            }
        }
        if (!rowOk)
        {
            break;
        }
    }

    totalBytesRead += tuplesRead * currentOffset;
    return tuplesRead;
}

bool ReplayStoreReader::isEof() const
{
    return inputFile.eof();
}

std::streampos ReplayStoreReader::getPosition()
{
    return inputFile.tellg();
}

void ReplayStoreReader::clearErrors()
{
    inputFile.clear();
}

void ReplayStoreReader::seekTo(std::streampos pos)
{
    inputFile.seekg(pos);
}

int ReplayStoreReader::peek()
{
    return inputFile.peek();
}

Schema ReplayStoreReader::readSchemaFromFile(const std::string& filePath)
{
    std::ifstream ifs(filePath, std::ios::binary);
    if (!ifs)
    {
        throw InvalidConfigParameter("ReplayStoreReader::readSchemaFromFile: cannot open file: {}", filePath);
    }
    auto [fileHeader, dataStart] = parseHeader(ifs);
    return parseSchemaFromText(fileHeader.schemaText);
}

} // namespace NES::Replay
