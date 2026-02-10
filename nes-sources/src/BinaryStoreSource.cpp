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

#include <Sources/BinaryStoreSource.hpp>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <regex>
#include <stop_token>
#include <string>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{
namespace
{

uint64_t parseHeader(std::ifstream& ifs)
{
    char magic[8];
    ifs.read(magic, 8);
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read magic from file");
    }
    NES_DEBUG(
        "BinaryStoreSource: magic read ok: {}{}{}{}{}{}{}{}",
        magic[0],
        magic[1],
        magic[2],
        magic[3],
        magic[4],
        magic[5],
        magic[6],
        magic[7]);
    uint32_t version = 0;
    uint8_t endianness = 0;
    uint32_t flags = 0;
    uint64_t fingerprint = 0;
    uint32_t schemaLen = 0;
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    ifs.read(reinterpret_cast<char*>(&endianness), sizeof(endianness));
    ifs.read(reinterpret_cast<char*>(&flags), sizeof(flags));
    ifs.read(reinterpret_cast<char*>(&fingerprint), sizeof(fingerprint));
    ifs.read(reinterpret_cast<char*>(&schemaLen), sizeof(schemaLen));
    if (!ifs)
    {
        throw CannotOpenSink("Failed to read header fields");
    }
    ifs.seekg(schemaLen, std::ios::cur);
    if (!ifs)
    {
        throw CannotOpenSink("Failed to skip schema bytes");
    }
    return static_cast<uint64_t>(8 + 4 + 1 + 4 + 8 + 4 + schemaLen);
}

}

BinaryStoreSource::BinaryStoreSource(const SourceDescriptor& sourceDescriptor)
    : filePath(
          sourceDescriptor.getConfig().contains("file_path") ? std::get<std::string>(sourceDescriptor.getConfig().at("file_path"))
                                                             : std::string())
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
{
}

void BinaryStoreSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_DEBUG("BinaryStoreSource: opening {}", filePath);
    inputFile = std::ifstream(filePath, std::ios::binary);
    if (!inputFile)
    {
        throw CannotOpenSink("Could not open input file: {}: {}", filePath, std::strerror(errno));
    }
    dataStartOffset = parseHeader(inputFile);
    NES_DEBUG("BinaryStoreSource: dataStartOffset={} currentPos={}", dataStartOffset, static_cast<long long>(inputFile.tellg()));
}

void BinaryStoreSource::close()
{
    inputFile.close();
}

Source::FillTupleBufferResult BinaryStoreSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    const auto startPos = inputFile.tellg();
    NES_DEBUG("BinaryStoreSource: fill called at pos {}", static_cast<long long>(startPos));
    if (!inputFile)
    {
        return FillTupleBufferResult::eos();
    }

    auto rowLayout = LowerSchemaProvider::lowerSchema(tupleBuffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);
    const auto capacity = rowLayout->getCapacity();
    uint64_t tuplesWritten = 0;

    std::vector<uint64_t> fieldOffsets;
    std::vector<uint64_t> fieldSizes;
    uint64_t currentOffset = 0;
    uint32_t rowWidthBytes = 0;

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
            size = type.getSizeInBytesWithNull();
        }
        fieldSizes.push_back(size);
        currentOffset += size;
        rowWidthBytes += size;
    }

    for (; tuplesWritten < capacity; ++tuplesWritten)
    {
        bool rowOk = true;
        for (size_t fieldIdx = 0; fieldIdx < schema.getNumberOfFields(); ++fieldIdx)
        {
            auto type = schema.getFieldAt(fieldIdx).dataType;
            const auto fieldSize = fieldSizes[fieldIdx];
            const auto offset = tuplesWritten * rowLayout->getTupleSize() + fieldOffsets[fieldIdx];
            char* dest = tupleBuffer.getAvailableMemoryArea<char>().data() + offset;

            if (type.isType(DataType::Type::VARSIZED))
            {
                /// TODO #11: Add support for varsized
                /// Expecting [u32 length][bytes]; skip content, write zero child index into row
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
                std::memset(dest, 0, fieldSize);
            }
            else
            {
                inputFile.read(dest, static_cast<std::streamsize>(fieldSize));
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

    const size_t bytesRead = static_cast<size_t>(tuplesWritten) * static_cast<size_t>(rowWidthBytes);
    tupleBuffer.setNumberOfTuples(tuplesWritten);

    bool reachedEnd = false;
    if (!inputFile)
    {
        const bool hitEof = inputFile.eof();
        inputFile.clear();
        if (startPos != -1)
        {
            inputFile.seekg(static_cast<std::streamoff>(startPos) + static_cast<std::streamoff>(bytesRead), std::ios::beg);
        }
        if (hitEof)
        {
            reachedEnd = true;
            tupleBuffer.setLastChunk(true);
        }
    }
    else
    {
        if (inputFile.peek() == std::char_traits<char>::eof())
        {
            reachedEnd = true;
            tupleBuffer.setLastChunk(true);
        }
    }

    NES_DEBUG("BinaryStoreSource: read {} bytes ({} tuples), eof={} fail={}", bytesRead, tuplesWritten, inputFile.eof(), inputFile.fail());
    totalNumBytesRead += bytesRead;

    if (tuplesWritten == 0 && reachedEnd)
    {
        return FillTupleBufferResult::eos();
    }
    /// Return the tuple count (not byte count) because NATIVE sources bypass the InputFormatter,
    /// and the SourceThread uses this value directly as setNumberOfTuples().
    return FillTupleBufferResult::withBytes(tuplesWritten);
}

uint32_t BinaryStoreSource::getRowWidthBytes() const
{
    uint32_t rowWidth = 0;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        auto type = schema.getFieldAt(i).dataType;
        if (type.isType(DataType::Type::VARSIZED))
        {
            rowWidth += sizeof(uint32_t);
        }
        else
        {
            rowWidth += type.getSizeInBytesWithoutNull();
        }
    }
    return rowWidth;
}

Schema BinaryStoreSource::readSchemaFromFile(const std::string& filePath)
{
    std::ifstream ifs(filePath, std::ios::binary);
    if (!ifs)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: cannot open file: {}", filePath);
    }
    constexpr int headerFixedBytes = 8 + 4 + 1 + 4 + 8;
    ifs.seekg(headerFixedBytes, std::ios::beg);
    uint32_t schemaLen = 0;
    ifs.read(reinterpret_cast<char*>(&schemaLen), sizeof(schemaLen));
    if (!ifs || schemaLen == 0)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: failed to read schema length");
    }
    std::string schemaText(schemaLen, '\0');
    ifs.read(schemaText.data(), schemaLen);
    if (!ifs)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: failed to read schema text");
    }

    Schema schema;
    const std::regex fieldRegex(R"(Field\(name:\s*([\w.$]+),\s*DataType:\s*DataType\(type:\s*(\w+)\)\))");
    auto begin = std::sregex_iterator(schemaText.begin(), schemaText.end(), fieldRegex);
    auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it)
    {
        const auto& match = *it;
        auto fieldName = match[1].str();
        if (const auto pos = fieldName.rfind('$'); pos != std::string::npos)
        {
            fieldName = fieldName.substr(pos + 1);
        }
        schema.addField(fieldName, DataTypeProvider::provideDataType(match[2].str()));
    }
    if (schema.getNumberOfFields() == 0)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: no fields parsed from: {}", schemaText);
    }
    return schema;
}

DescriptorConfig::Config BinaryStoreSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    if (config.find("file_path") == config.end())
    {
        throw InvalidConfigParameter("BinaryStoreSource requires 'file_path'");
    }
    DescriptorConfig::Config validated;
    validated.emplace("file_path", DescriptorConfig::ConfigType(config.at("file_path")));
    validated.emplace("number_of_buffers_in_local_pool", DescriptorConfig::ConfigType(static_cast<int64_t>(-1)));
    validated.emplace("max_inflight_buffers", DescriptorConfig::ConfigType(SourceDescriptor::INVALID_MAX_INFLIGHT_BUFFERS));
    return validated;
}

std::ostream& BinaryStoreSource::toString(std::ostream& str) const
{
    str << fmt::format("BinaryStoreSource(filePath: {}, bytesRead: {})", filePath, totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType RegisterBinaryStoreSourceValidation(SourceValidationRegistryArguments args)
{
    return BinaryStoreSource::validateAndFormat(std::move(args.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterBinaryStoreSource(SourceRegistryArguments args)
{
    return std::make_unique<BinaryStoreSource>(args.sourceDescriptor);
}
}
