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

#include <algorithm>
#include <array>
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
#include <Replay/ReplayStorage.hpp>
#include <Nautilus/Interface/BufferRef/RowTupleBufferRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

#include <zstd.h>

namespace NES
{
namespace
{
struct ParsedStoreHeader
{
    uint32_t version;
    Replay::BinaryStoreCompressionCodec compression;
    uint32_t schemaLen;
};

ParsedStoreHeader readStoreHeaderPrefix(std::istream& input)
{
    std::array<char, Replay::BINARY_STORE_MAGIC.size()> magic{};
    input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
    if (!input)
    {
        throw CannotOpenSink("Failed to read binary store magic");
    }
    if (!std::equal(magic.begin(), magic.end(), Replay::BINARY_STORE_MAGIC.begin()))
    {
        throw CannotOpenSink("Invalid binary store magic");
    }

    uint32_t version = 0;
    uint8_t endianness = 0;
    uint32_t flags = 0;
    [[maybe_unused]] uint64_t fingerprint = 0;
    uint32_t schemaLen = 0;
    input.read(reinterpret_cast<char*>(&version), sizeof(version));
    input.read(reinterpret_cast<char*>(&endianness), sizeof(endianness));
    input.read(reinterpret_cast<char*>(&flags), sizeof(flags));
    input.read(reinterpret_cast<char*>(&fingerprint), sizeof(fingerprint));
    input.read(reinterpret_cast<char*>(&schemaLen), sizeof(schemaLen));
    if (!input)
    {
        throw CannotOpenSink("Failed to read binary store header");
    }
    if (endianness != Replay::BINARY_STORE_ENDIANNESS_LITTLE)
    {
        throw CannotOpenSink("Unsupported binary store endianness: {}", static_cast<uint32_t>(endianness));
    }
    if (version != Replay::BINARY_STORE_FORMAT_VERSION_RAW_ROWS && version != Replay::BINARY_STORE_FORMAT_VERSION_SEGMENTED)
    {
        throw CannotOpenSink("Unsupported binary store format version: {}", version);
    }
    const auto compression = Replay::binaryStoreCodecFromFlags(flags);
    if (!compression.has_value())
    {
        throw CannotOpenSink("Unsupported binary store codec flags: {}", flags);
    }

    return ParsedStoreHeader{
        .version = version,
        .compression = *compression,
        .schemaLen = schemaLen,
    };
}

Schema parseSchemaText(const std::string& schemaText)
{
    Schema schema;
    const std::regex fieldRegex(R"(Field\(name:\s*([\w.$]+),\s*DataType:\s*DataType\(type:\s*(\w+)\)\))");
    const auto begin = std::sregex_iterator(schemaText.begin(), schemaText.end(), fieldRegex);
    const auto end = std::sregex_iterator();
    for (auto it = begin; it != end; ++it)
    {
        auto fieldName = (*it)[1].str();
        if (const auto pos = fieldName.rfind('$'); pos != std::string::npos)
        {
            fieldName = fieldName.substr(pos + 1);
        }
        schema.addField(fieldName, DataTypeProvider::provideDataType((*it)[2].str()));
    }
    if (schema.getNumberOfFields() == 0)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: no fields parsed from: {}", schemaText);
    }
    return schema;
}
}

BinaryStoreSource::BinaryStoreSource(const SourceDescriptor& sourceDescriptor)
    : filePath(
          sourceDescriptor.getConfig().contains("file_path") ? std::get<std::string>(sourceDescriptor.getConfig().at("file_path"))
                                                             : std::string())
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
{
    initializeRowLayoutMetadata();
}

void BinaryStoreSource::initializeRowLayoutMetadata()
{
    fieldOffsets.clear();
    fieldSizes.clear();
    fieldOffsets.reserve(schema.getNumberOfFields());
    fieldSizes.reserve(schema.getNumberOfFields());
    uint64_t currentOffset = 0;
    rowWidthBytes = 0;
    hasVarSizedFields = false;
    for (size_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        fieldOffsets.push_back(currentOffset);
        const auto type = schema.getFieldAt(i).dataType;
        uint64_t fieldSize = 0;
        if (type.isType(DataType::Type::VARSIZED))
        {
            fieldSize = sizeof(uint32_t);
            hasVarSizedFields = true;
        }
        else
        {
            fieldSize = type.getSizeInBytes();
        }
        fieldSizes.push_back(fieldSize);
        currentOffset += fieldSize;
        rowWidthBytes += static_cast<uint32_t>(fieldSize);
    }
}

void BinaryStoreSource::recordPhysicalBytesRead(const size_t bytes)
{
    totalNumBytesRead += bytes;
    Replay::recordReplayReadBytes(bytes);
}

void BinaryStoreSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    NES_DEBUG("BinaryStoreSource: opening {}", filePath);
    inputFile = std::ifstream(filePath, std::ios::binary);
    if (!inputFile)
    {
        throw CannotOpenSink("Could not open input file: {}: {}", filePath, std::strerror(errno));
    }

    const auto header = readStoreHeaderPrefix(inputFile);
    formatVersion = header.version;
    compression = header.compression;
    inputFile.seekg(static_cast<std::streamoff>(header.schemaLen), std::ios::cur);
    if (!inputFile)
    {
        throw CannotOpenSink("Failed to skip schema bytes");
    }
    dataStartOffset = static_cast<uint64_t>(inputFile.tellg());
    segmentBuffer.clear();
    segmentBufferOffset = 0;
    endOfSegmentStream = false;
    NES_DEBUG(
        "BinaryStoreSource: formatVersion={} compression={} dataStartOffset={}",
        formatVersion,
        static_cast<uint32_t>(compression),
        dataStartOffset);
}

void BinaryStoreSource::close()
{
    inputFile.close();
    segmentBuffer.clear();
    segmentBufferOffset = 0;
    endOfSegmentStream = false;
}

bool BinaryStoreSource::loadNextSegment()
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion) || endOfSegmentStream)
    {
        return false;
    }

    Replay::BinaryStoreSegmentHeader segmentHeader{};
    inputFile.read(reinterpret_cast<char*>(&segmentHeader.decodedSize), sizeof(segmentHeader.decodedSize));
    if (!inputFile)
    {
        if (inputFile.eof())
        {
            inputFile.clear();
            endOfSegmentStream = true;
            return false;
        }
        throw CannotOpenSink("Failed to read binary store segment header");
    }
    inputFile.read(reinterpret_cast<char*>(&segmentHeader.encodedSize), sizeof(segmentHeader.encodedSize));
    if (!inputFile)
    {
        throw CannotOpenSink("Failed to read binary store segment size");
    }
    if (segmentHeader.decodedSize == 0 || segmentHeader.encodedSize == 0)
    {
        throw CannotOpenSink("Binary store segment sizes must be greater than zero");
    }

    std::vector<uint8_t> encodedSegment(segmentHeader.encodedSize);
    inputFile.read(reinterpret_cast<char*>(encodedSegment.data()), static_cast<std::streamsize>(encodedSegment.size()));
    if (!inputFile)
    {
        throw CannotOpenSink("Failed to read binary store segment payload");
    }
    recordPhysicalBytesRead(encodedSegment.size());

    segmentBufferOffset = 0;
    if (compression == Replay::BinaryStoreCompressionCodec::None)
    {
        if (segmentHeader.encodedSize != segmentHeader.decodedSize)
        {
            throw CannotOpenSink("Uncompressed binary store segment has mismatched encoded and decoded sizes");
        }
        segmentBuffer = std::move(encodedSegment);
    }
    else if (compression == Replay::BinaryStoreCompressionCodec::Zstd)
    {
        segmentBuffer.resize(segmentHeader.decodedSize);
        const auto decompressedSize = ZSTD_decompress(
            segmentBuffer.data(), segmentBuffer.size(), encodedSegment.data(), encodedSegment.size());
        if (ZSTD_isError(decompressedSize))
        {
            throw CannotOpenSink("ZSTD_decompress failed: {}", ZSTD_getErrorName(decompressedSize));
        }
        if (decompressedSize != segmentBuffer.size())
        {
            throw CannotOpenSink(
                "Binary store segment decompressed to {} bytes but header declared {}",
                decompressedSize,
                segmentBuffer.size());
        }
    }
    else
    {
        throw CannotOpenSink("Unsupported binary store compression codec: {}", static_cast<uint32_t>(compression));
    }

    if (!hasVarSizedFields && rowWidthBytes > 0 && (segmentBuffer.size() % rowWidthBytes) != 0)
    {
        throw CannotOpenSink(
            "Binary store segment size {} is not a multiple of row width {}",
            segmentBuffer.size(),
            rowWidthBytes);
    }
    return true;
}

bool BinaryStoreSource::readPayload(char* dest, size_t len)
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion))
    {
        inputFile.read(dest, static_cast<std::streamsize>(len));
        if (!inputFile)
        {
            inputFile.clear();
            return false;
        }
        recordPhysicalBytesRead(len);
        return true;
    }

    size_t copied = 0;
    while (copied < len)
    {
        if (segmentBufferOffset >= segmentBuffer.size())
        {
            segmentBuffer.clear();
            segmentBufferOffset = 0;
            if (!loadNextSegment())
            {
                return false;
            }
        }
        const auto available = segmentBuffer.size() - segmentBufferOffset;
        const auto toCopy = std::min(available, len - copied);
        std::memcpy(dest + copied, segmentBuffer.data() + segmentBufferOffset, toCopy);
        copied += toCopy;
        segmentBufferOffset += toCopy;
    }
    return true;
}

bool BinaryStoreSource::skipPayload(size_t len)
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion))
    {
        inputFile.seekg(static_cast<std::streamoff>(len), std::ios::cur);
        if (!inputFile)
        {
            inputFile.clear();
            return false;
        }
        recordPhysicalBytesRead(len);
        return true;
    }

    size_t skipped = 0;
    while (skipped < len)
    {
        if (segmentBufferOffset >= segmentBuffer.size())
        {
            segmentBuffer.clear();
            segmentBufferOffset = 0;
            if (!loadNextSegment())
            {
                return false;
            }
        }
        const auto available = segmentBuffer.size() - segmentBufferOffset;
        const auto toSkip = std::min(available, len - skipped);
        skipped += toSkip;
        segmentBufferOffset += toSkip;
    }
    return true;
}

bool BinaryStoreSource::isPayloadExhausted()
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion))
    {
        return inputFile.peek() == std::char_traits<char>::eof();
    }
    if (segmentBufferOffset < segmentBuffer.size())
    {
        return false;
    }
    if (endOfSegmentStream)
    {
        return true;
    }
    segmentBuffer.clear();
    segmentBufferOffset = 0;
    return !loadNextSegment();
}

Source::FillTupleBufferResult BinaryStoreSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    NES_DEBUG("BinaryStoreSource: fill called at pos {}", static_cast<long long>(inputFile.tellg()));
    if (!inputFile)
    {
        return FillTupleBufferResult::eos();
    }

    auto rowLayout = LowerSchemaProvider::lowerSchema(tupleBuffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);
    const auto capacity = rowLayout->getCapacity();
    uint64_t tuplesWritten = 0;

    for (; tuplesWritten < capacity; ++tuplesWritten)
    {
        bool rowOk = true;
        for (size_t fieldIdx = 0; fieldIdx < schema.getNumberOfFields(); ++fieldIdx)
        {
            const auto type = schema.getFieldAt(fieldIdx).dataType;
            const auto fieldSize = fieldSizes[fieldIdx];
            const auto offset = tuplesWritten * rowLayout->getTupleSize() + fieldOffsets[fieldIdx];
            char* dest = tupleBuffer.getAvailableMemoryArea<char>().data() + offset;

            if (type.isType(DataType::Type::VARSIZED))
            {
                /// TODO #11: Add support for varsized
                uint32_t len = 0;
                rowOk = readPayload(reinterpret_cast<char*>(&len), sizeof(uint32_t));
                if (!rowOk)
                {
                    break;
                }
                rowOk = skipPayload(len);
                if (!rowOk)
                {
                    break;
                }
                std::memset(dest, 0, fieldSize);
            }
            else
            {
                rowOk = readPayload(dest, fieldSize);
                if (!rowOk)
                {
                    break;
                }
            }
        }
        if (!rowOk)
        {
            break;
        }
    }

    const auto bytesRead = static_cast<size_t>(tuplesWritten) * static_cast<size_t>(rowWidthBytes);
    tupleBuffer.setNumberOfTuples(tuplesWritten);
    const auto reachedEnd = isPayloadExhausted();
    if (reachedEnd)
    {
        tupleBuffer.setLastChunk(true);
    }

    NES_DEBUG("BinaryStoreSource: read {} bytes ({} tuples), eof={} fail={}", bytesRead, tuplesWritten, inputFile.eof(), inputFile.fail());

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
    return rowWidthBytes;
}

Schema BinaryStoreSource::readSchemaFromFile(const std::string& filePath)
{
    std::ifstream input(filePath, std::ios::binary);
    if (!input)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: cannot open file: {}", filePath);
    }

    const auto header = readStoreHeaderPrefix(input);
    if (header.schemaLen == 0)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: failed to read schema length");
    }
    std::string schemaText(header.schemaLen, '\0');
    input.read(schemaText.data(), static_cast<std::streamsize>(schemaText.size()));
    if (!input)
    {
        throw InvalidConfigParameter("BinaryStoreSource::readSchemaFromFile: failed to read schema text");
    }
    return parseSchemaText(schemaText);
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
