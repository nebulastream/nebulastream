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
#include <optional>
#include <sstream>
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
#include <Util/Strings.hpp>
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

enum class BinaryStoreReplayMode : uint8_t
{
    Full,
    Interval,
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

std::optional<uint64_t> readOptionalUInt64Config(const DescriptorConfig::Config& config, const std::string_view key)
{
    const auto it = config.find(std::string(key));
    if (it == config.end())
    {
        return std::nullopt;
    }

    if (const auto* value = std::get_if<uint64_t>(&it->second))
    {
        return *value;
    }
    if (const auto* value = std::get_if<int64_t>(&it->second))
    {
        if (*value < 0)
        {
            throw InvalidConfigParameter("BinaryStoreSource config '{}' must be non-negative", key);
        }
        return static_cast<uint64_t>(*value);
    }
    if (const auto* value = std::get_if<uint32_t>(&it->second))
    {
        return *value;
    }
    if (const auto* value = std::get_if<int32_t>(&it->second))
    {
        if (*value < 0)
        {
            throw InvalidConfigParameter("BinaryStoreSource config '{}' must be non-negative", key);
        }
        return static_cast<uint64_t>(*value);
    }
    if (const auto* value = std::get_if<std::string>(&it->second))
    {
        const auto parsed = from_chars<uint64_t>(*value);
        if (!parsed.has_value())
        {
            throw InvalidConfigParameter("BinaryStoreSource config '{}' must be an unsigned integer", key);
        }
        return *parsed;
    }

    throw InvalidConfigParameter("BinaryStoreSource config '{}' has an unsupported type", key);
}

std::optional<std::string> readOptionalStringConfig(const DescriptorConfig::Config& config, const std::string_view key)
{
    const auto it = config.find(std::string(key));
    if (it == config.end())
    {
        return std::nullopt;
    }
    if (const auto* value = std::get_if<std::string>(&it->second))
    {
        return *value;
    }
    throw InvalidConfigParameter("BinaryStoreSource config '{}' must be a string", key);
}

std::optional<std::vector<uint64_t>> parseSegmentIds(std::string_view rawSegmentIds)
{
    std::vector<uint64_t> segmentIds;
    for (const auto part : splitOnMultipleDelimiters(rawSegmentIds, {',', ';', ' ', '\t', '\n'}))
    {
        const auto parsedSegmentId = from_chars<uint64_t>(part);
        if (!parsedSegmentId.has_value())
        {
            return std::nullopt;
        }
        if (!std::ranges::contains(segmentIds, *parsedSegmentId))
        {
            segmentIds.push_back(*parsedSegmentId);
        }
    }
    if (segmentIds.empty())
    {
        return std::nullopt;
    }
    return segmentIds;
}

std::string normalizeSegmentIds(std::string_view rawSegmentIds)
{
    const auto parsedSegmentIds = parseSegmentIds(rawSegmentIds);
    if (!parsedSegmentIds.has_value())
    {
        throw InvalidConfigParameter("BinaryStoreSource config 'segment_ids' must contain a comma-separated list of uint64 ids");
    }

    std::ostringstream normalized;
    for (size_t index = 0; index < parsedSegmentIds->size(); ++index)
    {
        if (index > 0)
        {
            normalized << ',';
        }
        normalized << parsedSegmentIds->at(index);
    }
    return normalized.str();
}

std::optional<BinaryStoreReplayMode> parseReplayMode(std::string_view replayMode)
{
    const auto normalizedReplayMode = toLowerCase(trimWhiteSpaces(replayMode));
    if (normalizedReplayMode.empty() || normalizedReplayMode == "full")
    {
        return BinaryStoreReplayMode::Full;
    }
    if (normalizedReplayMode == "interval")
    {
        return BinaryStoreReplayMode::Interval;
    }
    return std::nullopt;
}

bool hasReplaySelectionFilters(const Replay::BinaryStoreReplaySelection& replaySelection)
{
    return replaySelection.segmentIds.has_value() || replaySelection.scanStartMs.has_value() || replaySelection.scanEndMs.has_value();
}

std::vector<uint64_t> selectedSegmentIds(const std::vector<Replay::BinaryStoreManifestEntry>& selectedSegments)
{
    return selectedSegments | std::views::transform([](const auto& segment) { return segment.segmentId; }) | std::ranges::to<std::vector>();
}
}

BinaryStoreSource::BinaryStoreSource(const SourceDescriptor& sourceDescriptor)
    : filePath(
          sourceDescriptor.getConfig().contains("file_path")
              ? std::get<std::string>(sourceDescriptor.getConfig().at("file_path"))
              : Replay::getRecordingFilePath(std::get<std::string>(sourceDescriptor.getConfig().at("recording_id"))))
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
{
    replaySelection.segmentIds = readOptionalStringConfig(sourceDescriptor.getConfig(), "segment_ids").transform(
        [](const auto& rawSegmentIds)
        {
            const auto parsedSegmentIds = parseSegmentIds(rawSegmentIds);
            PRECONDITION(parsedSegmentIds.has_value(), "BinaryStoreSource segment_ids should have been validated before construction");
            return *parsedSegmentIds;
        });
    replaySelection.scanStartMs = readOptionalUInt64Config(sourceDescriptor.getConfig(), "scan_start_ms");
    replaySelection.scanEndMs = readOptionalUInt64Config(sourceDescriptor.getConfig(), "scan_end_ms");
    const auto replayMode = readOptionalStringConfig(sourceDescriptor.getConfig(), "replay_mode").transform(
        [](const auto& rawReplayMode)
        {
            const auto parsedReplayMode = parseReplayMode(rawReplayMode);
            PRECONDITION(parsedReplayMode.has_value(), "BinaryStoreSource replay_mode should have been validated before construction");
            return *parsedReplayMode;
        });

    shouldPinReplaySegments = Replay::binaryStoreManifestExists(filePath) || sourceDescriptor.getConfig().contains("recording_id");
    useManifestSelection = replayMode == BinaryStoreReplayMode::Interval || hasReplaySelectionFilters(replaySelection);
    initializeRowLayoutMetadata();
}

BinaryStoreSource::~BinaryStoreSource()
{
    try
    {
        close();
    }
    catch (const std::exception& exception)
    {
        NES_WARNING("BinaryStoreSource failed to release replay pins for {}: {}", filePath, exception.what());
    }
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
    close();
    NES_DEBUG("BinaryStoreSource: opening {}", filePath);
    selectedSegments.clear();
    nextSelectedSegmentIndex = 0;
    currentSelectedRawSegmentBytesRemaining = 0;
    if (useManifestSelection)
    {
        if (!Replay::binaryStoreManifestExists(filePath))
        {
            throw CannotOpenSink(
                "BinaryStoreSource replay segment selection requires a manifest-backed recording, but {} has no manifest",
                filePath);
        }
        selectedSegments = Replay::selectBinaryStoreSegments(filePath, replaySelection);
        pinnedSegmentIds = Replay::pinBinaryStoreSegments(filePath, selectedSegmentIds(selectedSegments));
    }
    else if (shouldPinReplaySegments)
    {
        pinnedSegmentIds = Replay::pinBinaryStoreSegments(filePath);
    }
    try
    {
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
        selectedSegments.shrink_to_fit();
        nextSelectedSegmentIndex = 0;
        currentSelectedRawSegmentBytesRemaining = 0;
        endOfSegmentStream = useManifestSelection && selectedSegments.empty();
        if (useManifestSelection && !Replay::binaryStoreUsesSegmentedPayload(formatVersion))
        {
            advanceToNextSelectedRawSegment();
        }
        NES_DEBUG(
            "BinaryStoreSource: formatVersion={} compression={} dataStartOffset={}",
            formatVersion,
            static_cast<uint32_t>(compression),
            dataStartOffset);
    }
    catch (...)
    {
        if (inputFile.is_open())
        {
            inputFile.close();
        }
        if (!pinnedSegmentIds.empty())
        {
            Replay::unpinBinaryStoreSegments(filePath, pinnedSegmentIds);
            pinnedSegmentIds.clear();
        }
        throw;
    }
}

void BinaryStoreSource::close()
{
    if (inputFile.is_open())
    {
        inputFile.close();
    }
    segmentBuffer.clear();
    segmentBufferOffset = 0;
    endOfSegmentStream = false;
    selectedSegments.clear();
    nextSelectedSegmentIndex = 0;
    currentSelectedRawSegmentBytesRemaining = 0;
    if (!pinnedSegmentIds.empty())
    {
        Replay::unpinBinaryStoreSegments(filePath, pinnedSegmentIds);
        pinnedSegmentIds.clear();
    }
}

bool BinaryStoreSource::loadNextSegment()
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion) || endOfSegmentStream)
    {
        return false;
    }

    Replay::BinaryStoreSegmentHeader segmentHeader{};
    if (useManifestSelection)
    {
        if (nextSelectedSegmentIndex >= selectedSegments.size())
        {
            endOfSegmentStream = true;
            return false;
        }

        const auto& selectedSegment = selectedSegments.at(nextSelectedSegmentIndex++);
        inputFile.seekg(static_cast<std::streamoff>(selectedSegment.payloadOffset), std::ios::beg);
        if (!inputFile)
        {
            throw CannotOpenSink("Failed to seek binary store segment {} at offset {}", selectedSegment.segmentId, selectedSegment.payloadOffset);
        }
        inputFile.read(reinterpret_cast<char*>(&segmentHeader.decodedSize), sizeof(segmentHeader.decodedSize));
        if (!inputFile)
        {
            throw CannotOpenSink("Failed to read binary store segment {} header", selectedSegment.segmentId);
        }
        inputFile.read(reinterpret_cast<char*>(&segmentHeader.encodedSize), sizeof(segmentHeader.encodedSize));
        if (!inputFile)
        {
            throw CannotOpenSink("Failed to read binary store segment {} size", selectedSegment.segmentId);
        }
        const auto expectedStoredSize = sizeof(Replay::BinaryStoreSegmentHeader) + static_cast<uint64_t>(segmentHeader.encodedSize);
        if (expectedStoredSize != selectedSegment.storedSizeBytes)
        {
            throw CannotOpenSink(
                "Binary store segment {} header declares {} stored bytes but manifest declares {}",
                selectedSegment.segmentId,
                expectedStoredSize,
                selectedSegment.storedSizeBytes);
        }
    }
    else
    {
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

bool BinaryStoreSource::advanceToNextSelectedRawSegment()
{
    if (!useManifestSelection)
    {
        return false;
    }
    if (nextSelectedSegmentIndex >= selectedSegments.size())
    {
        currentSelectedRawSegmentBytesRemaining = 0;
        return false;
    }

    const auto& selectedSegment = selectedSegments.at(nextSelectedSegmentIndex++);
    inputFile.seekg(static_cast<std::streamoff>(selectedSegment.payloadOffset), std::ios::beg);
    if (!inputFile)
    {
        throw CannotOpenSink("Failed to seek binary store segment {} at offset {}", selectedSegment.segmentId, selectedSegment.payloadOffset);
    }
    currentSelectedRawSegmentBytesRemaining = selectedSegment.storedSizeBytes;
    return true;
}

bool BinaryStoreSource::readPayload(char* dest, size_t len)
{
    if (!Replay::binaryStoreUsesSegmentedPayload(formatVersion))
    {
        if (useManifestSelection)
        {
            size_t copied = 0;
            while (copied < len)
            {
                if (currentSelectedRawSegmentBytesRemaining == 0 && !advanceToNextSelectedRawSegment())
                {
                    return false;
                }

                const auto toRead
                    = static_cast<size_t>(std::min<uint64_t>(currentSelectedRawSegmentBytesRemaining, static_cast<uint64_t>(len - copied)));
                inputFile.read(dest + copied, static_cast<std::streamsize>(toRead));
                if (!inputFile)
                {
                    inputFile.clear();
                    return false;
                }
                recordPhysicalBytesRead(toRead);
                copied += toRead;
                currentSelectedRawSegmentBytesRemaining -= toRead;
            }
            return true;
        }

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
        if (useManifestSelection)
        {
            size_t skipped = 0;
            while (skipped < len)
            {
                if (currentSelectedRawSegmentBytesRemaining == 0 && !advanceToNextSelectedRawSegment())
                {
                    return false;
                }

                const auto toSkip
                    = static_cast<size_t>(std::min<uint64_t>(currentSelectedRawSegmentBytesRemaining, static_cast<uint64_t>(len - skipped)));
                inputFile.seekg(static_cast<std::streamoff>(toSkip), std::ios::cur);
                if (!inputFile)
                {
                    inputFile.clear();
                    return false;
                }
                recordPhysicalBytesRead(toSkip);
                skipped += toSkip;
                currentSelectedRawSegmentBytesRemaining -= toSkip;
            }
            return true;
        }

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
        if (useManifestSelection)
        {
            return currentSelectedRawSegmentBytesRemaining == 0 && nextSelectedSegmentIndex >= selectedSegments.size();
        }
        return inputFile.peek() == std::char_traits<char>::eof();
    }
    if (segmentBufferOffset < segmentBuffer.size())
    {
        return false;
    }
    if (useManifestSelection)
    {
        return nextSelectedSegmentIndex >= selectedSegments.size();
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
    if (config.find("file_path") == config.end() && config.find("recording_id") == config.end())
    {
        throw InvalidConfigParameter("BinaryStoreSource requires 'file_path' or 'recording_id'");
    }
    DescriptorConfig::Config validated;
    if (const auto filePathIt = config.find("file_path"); filePathIt != config.end())
    {
        validated.emplace("file_path", DescriptorConfig::ConfigType(filePathIt->second));
    }
    if (const auto recordingIdIt = config.find("recording_id"); recordingIdIt != config.end())
    {
        validated.emplace("recording_id", DescriptorConfig::ConfigType(recordingIdIt->second));
    }
    if (const auto segmentIdsIt = config.find("segment_ids"); segmentIdsIt != config.end())
    {
        validated.emplace("segment_ids", DescriptorConfig::ConfigType(normalizeSegmentIds(segmentIdsIt->second)));
    }
    if (const auto scanStartMsIt = config.find("scan_start_ms"); scanStartMsIt != config.end())
    {
        const auto scanStartMs = from_chars<uint64_t>(scanStartMsIt->second);
        if (!scanStartMs.has_value())
        {
            throw InvalidConfigParameter("BinaryStoreSource config 'scan_start_ms' must be an unsigned integer");
        }
        validated.emplace("scan_start_ms", DescriptorConfig::ConfigType(*scanStartMs));
    }
    if (const auto scanEndMsIt = config.find("scan_end_ms"); scanEndMsIt != config.end())
    {
        const auto scanEndMs = from_chars<uint64_t>(scanEndMsIt->second);
        if (!scanEndMs.has_value())
        {
            throw InvalidConfigParameter("BinaryStoreSource config 'scan_end_ms' must be an unsigned integer");
        }
        validated.emplace("scan_end_ms", DescriptorConfig::ConfigType(*scanEndMs));
    }
    if (const auto scanStartMs = readOptionalUInt64Config(validated, "scan_start_ms");
        scanStartMs.has_value())
    {
        if (const auto scanEndMs = readOptionalUInt64Config(validated, "scan_end_ms");
            scanEndMs.has_value() && *scanStartMs > *scanEndMs)
        {
            throw InvalidConfigParameter(
                "BinaryStoreSource config 'scan_start_ms' ({}) must be less than or equal to 'scan_end_ms' ({})",
                *scanStartMs,
                *scanEndMs);
        }
    }
    if (const auto replayModeIt = config.find("replay_mode"); replayModeIt != config.end())
    {
        const auto normalizedReplayMode = toLowerCase(trimWhiteSpaces(replayModeIt->second));
        if (!parseReplayMode(normalizedReplayMode).has_value())
        {
            throw InvalidConfigParameter("BinaryStoreSource config 'replay_mode' must be one of: full, interval");
        }
        const auto hasBoundedSelection = config.contains("segment_ids") || config.contains("scan_start_ms") || config.contains("scan_end_ms");
        if (normalizedReplayMode == "full" && hasBoundedSelection)
        {
            throw InvalidConfigParameter(
                "BinaryStoreSource config 'replay_mode=full' cannot be combined with 'segment_ids' or scan interval bounds");
        }
        validated.emplace("replay_mode", DescriptorConfig::ConfigType(normalizedReplayMode));
    }
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
