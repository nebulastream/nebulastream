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

#include <StoreOperatorHandler.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <Runtime/QueryTerminationType.hpp>
#include <Replay/ReplayStorage.hpp>
#include <ErrorHandling.hpp>
#include "Runtime/Execution/OperatorHandler.hpp"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zstd.h>

namespace NES
{

namespace
{
struct ParsedStoreHeader
{
    uint32_t version;
    uint32_t flags;
    Replay::BinaryStoreCompressionCodec compression;
    uint64_t schemaFingerprint;
};

ParsedStoreHeader parseStoreHeader(std::istream& input)
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
    uint64_t fingerprint = 0;
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

    input.seekg(static_cast<std::streamoff>(schemaLen), std::ios::cur);
    if (!input)
    {
        throw CannotOpenSink("Failed to skip binary store schema bytes");
    }

    return ParsedStoreHeader{
        .version = version,
        .flags = flags,
        .compression = *compression,
        .schemaFingerprint = fingerprint,
    };
}

uint32_t versionForCodec(const Replay::BinaryStoreCompressionCodec codec)
{
    return codec == Replay::BinaryStoreCompressionCodec::None ? Replay::BINARY_STORE_FORMAT_VERSION_RAW_ROWS
                                                              : Replay::BINARY_STORE_FORMAT_VERSION_SEGMENTED;
}

uint32_t chunkSizeBytes(const StoreOperatorHandler::Config& config)
{
    return std::max<uint32_t>(config.chunkMinBytes, 1);
}

std::string temporaryCompactionPath(const std::string& path)
{
    return path + ".gc.tmp";
}

void copyFileRange(std::ifstream& input, std::ofstream& output, const uint64_t offset, uint64_t size)
{
    static constexpr size_t COPY_BUFFER_BYTES = 1U << 20U;
    std::vector<char> buffer(static_cast<size_t>(std::min<uint64_t>(COPY_BUFFER_BYTES, std::max<uint64_t>(size, 1))));
    input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!input)
    {
        throw CannotOpenSink("Failed to seek recording payload at offset {}", offset);
    }

    while (size > 0)
    {
        const auto chunkBytes = static_cast<std::streamsize>(std::min<uint64_t>(size, buffer.size()));
        input.read(buffer.data(), chunkBytes);
        if (!input)
        {
            throw CannotOpenSink("Failed to read {} bytes from recording payload", chunkBytes);
        }
        output.write(buffer.data(), chunkBytes);
        if (!output)
        {
            throw CannotOpenSink("Failed to write {} bytes to compacted recording payload", chunkBytes);
        }
        size -= static_cast<uint64_t>(chunkBytes);
    }
}

void writeManifestFile(const std::string& manifestPath, const Replay::BinaryStoreManifest& manifest)
{
    std::ofstream manifestOutput(manifestPath, std::ios::trunc);
    if (!manifestOutput)
    {
        throw CannotOpenSink("Could not create binary store manifest {}", manifestPath);
    }

    manifestOutput << Replay::BINARY_STORE_MANIFEST_MAGIC << '\n';
    for (const auto& entry : manifest.segments)
    {
        manifestOutput << entry.segmentId << ' ' << entry.payloadOffset << ' ' << entry.storedSizeBytes << ' ' << entry.logicalSizeBytes
                       << ' ' << entry.minWatermark << ' ' << entry.maxWatermark << '\n';
    }
    if (!manifestOutput)
    {
        throw CannotOpenSink("Failed to write binary store manifest {}", manifestPath);
    }
}

Timestamp retentionCutoff(const Timestamp currentWatermark, const uint64_t retentionWindowMs)
{
    if (currentWatermark.getRawValue() <= retentionWindowMs)
    {
        return Timestamp(Timestamp::INITIAL_VALUE);
    }
    return currentWatermark - retentionWindowMs;
}
}

StoreOperatorHandler::StoreOperatorHandler(Config cfg) : config(std::move(cfg))
{
    if (config.compression != Replay::BinaryStoreCompressionCodec::None && !config.header)
    {
        throw CannotOpenSink("Compressed store files require header=true");
    }
    if (config.compression != Replay::BinaryStoreCompressionCodec::None && config.directIO)
    {
        throw CannotOpenSink("Compressed store files do not support direct_io=true");
    }
    if (config.compression != Replay::BinaryStoreCompressionCodec::None && config.chunkMinBytes == 0)
    {
        throw CannotOpenSink("Compressed store files require chunk_min_bytes > 0");
    }
    if (config.compression == Replay::BinaryStoreCompressionCodec::Zstd)
    {
        pendingSegment.reserve(config.chunkMinBytes);
    }
}

void StoreOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
    openFile(!config.append);
    initializeManifestState(!config.append || tail.load(std::memory_order_relaxed) == 0);
    if (config.header)
    {
        writeHeaderIfNeeded();
    }
}

void StoreOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    if (fd >= 0)
    {
        std::lock_guard lock(pendingSegmentMutex);
        if (config.compression != Replay::BinaryStoreCompressionCodec::None)
        {
            flushPendingSegment();
        }
        else
        {
            flushRawManifestSegment();
        }
        ::fsync(fd);
        ::close(fd);
        fd = -1;
    }
}

void StoreOperatorHandler::ensureHeader(PipelineExecutionContext&)
{
    if (config.header)
    {
        writeHeaderIfNeeded();
    }
}

void StoreOperatorHandler::append(const uint8_t* data, size_t len, Timestamp watermark)
{
    if (len == 0)
    {
        return;
    }
    if (fd < 0)
    {
        throw CannotOpenSink("StoreOperatorHandler not started; fd is invalid");
    }
    std::lock_guard lock(pendingSegmentMutex);
    if (config.compression == Replay::BinaryStoreCompressionCodec::None)
    {
        const auto payloadOffset = writeAtTail(data, len);
        if (currentSegmentLogicalSizeBytes == 0)
        {
            currentSegmentOffset = payloadOffset;
        }
        currentSegmentStoredSizeBytes += len;
        currentSegmentLogicalSizeBytes += len;
        updateCurrentSegmentWatermark(watermark);
        if (currentSegmentLogicalSizeBytes >= chunkSizeBytes(config))
        {
            flushRawManifestSegment();
        }
        return;
    }

    pendingSegment.insert(pendingSegment.end(), data, data + len);
    updateCurrentSegmentWatermark(watermark);
    if (pendingSegment.size() >= config.chunkMinBytes)
    {
        flushPendingSegment();
    }
}

void StoreOperatorHandler::flushPendingSegment()
{
    if (config.compression == Replay::BinaryStoreCompressionCodec::None || pendingSegment.empty())
    {
        return;
    }
    PRECONDITION(config.compression == Replay::BinaryStoreCompressionCodec::Zstd, "Unsupported binary store compression codec");
    if (pendingSegment.size() > std::numeric_limits<uint32_t>::max())
    {
        throw CannotOpenSink("Binary store segment exceeds supported size: {}", pendingSegment.size());
    }

    const auto compressedBound = ZSTD_compressBound(pendingSegment.size());
    std::vector<uint8_t> frame(sizeof(Replay::BinaryStoreSegmentHeader) + compressedBound);
    const auto compressedSize = ZSTD_compress(
        frame.data() + sizeof(Replay::BinaryStoreSegmentHeader),
        compressedBound,
        pendingSegment.data(),
        pendingSegment.size(),
        config.compressionLevel);
    if (ZSTD_isError(compressedSize))
    {
        throw CannotOpenSink("ZSTD_compress failed: {}", ZSTD_getErrorName(compressedSize));
    }
    if (compressedSize > std::numeric_limits<uint32_t>::max())
    {
        throw CannotOpenSink("Compressed binary store segment exceeds supported size: {}", compressedSize);
    }

    auto* segmentHeader = reinterpret_cast<Replay::BinaryStoreSegmentHeader*>(frame.data());
    segmentHeader->decodedSize = static_cast<uint32_t>(pendingSegment.size());
    segmentHeader->encodedSize = static_cast<uint32_t>(compressedSize);
    frame.resize(sizeof(Replay::BinaryStoreSegmentHeader) + compressedSize);

    const auto payloadOffset = writeAtTail(frame.data(), frame.size());
    PRECONDITION(currentSegmentMaxWatermark.has_value(), "Compressed segment requires a maximum watermark");
    const auto segmentMaxWatermark = *currentSegmentMaxWatermark;
    appendManifestEntry(payloadOffset, frame.size(), pendingSegment.size());
    pendingSegment.clear();
    resetCurrentSegment();
    maybeGarbageCollectExpiredSegments(segmentMaxWatermark);
}

void StoreOperatorHandler::flushRawManifestSegment()
{
    if (currentSegmentLogicalSizeBytes == 0)
    {
        return;
    }
    PRECONDITION(currentSegmentMaxWatermark.has_value(), "Raw segment requires a maximum watermark");
    const auto segmentMaxWatermark = *currentSegmentMaxWatermark;
    appendManifestEntry(currentSegmentOffset, currentSegmentStoredSizeBytes, currentSegmentLogicalSizeBytes);
    resetCurrentSegment();
    maybeGarbageCollectExpiredSegments(segmentMaxWatermark);
}

void StoreOperatorHandler::resetCurrentSegment()
{
    currentSegmentOffset = 0;
    currentSegmentStoredSizeBytes = 0;
    currentSegmentLogicalSizeBytes = 0;
    currentSegmentMinWatermark.reset();
    currentSegmentMaxWatermark.reset();
}

void StoreOperatorHandler::updateCurrentSegmentWatermark(const Timestamp watermark)
{
    currentSegmentMinWatermark = currentSegmentMinWatermark.has_value() ? std::min(*currentSegmentMinWatermark, watermark) : watermark;
    currentSegmentMaxWatermark = currentSegmentMaxWatermark.has_value() ? std::max(*currentSegmentMaxWatermark, watermark) : watermark;
}

void StoreOperatorHandler::appendManifestEntry(const uint64_t payloadOffset, const uint64_t storedSizeBytes, const uint64_t logicalSizeBytes)
{
    PRECONDITION(currentSegmentMinWatermark.has_value(), "Manifest entry requires a minimum watermark");
    PRECONDITION(currentSegmentMaxWatermark.has_value(), "Manifest entry requires a maximum watermark");

    const auto manifestPath = Replay::getRecordingManifestPath(config.filePath);
    std::ofstream manifestOutput(manifestPath, std::ios::app);
    if (!manifestOutput)
    {
        throw CannotOpenSink("Could not open binary store manifest {}", manifestPath);
    }

    manifestOutput << nextSegmentId << ' ' << payloadOffset << ' ' << storedSizeBytes << ' ' << logicalSizeBytes << ' '
                   << currentSegmentMinWatermark->getRawValue() << ' ' << currentSegmentMaxWatermark->getRawValue() << '\n';
    if (!manifestOutput)
    {
        throw CannotOpenSink("Failed to append binary store manifest {}", manifestPath);
    }
    ++nextSegmentId;
}

void StoreOperatorHandler::maybeGarbageCollectExpiredSegments(const Timestamp currentWatermark)
{
    if (!config.retentionWindowMs.has_value())
    {
        return;
    }

    const auto manifest = Replay::readBinaryStoreManifest(config.filePath);
    if (manifest.segments.empty())
    {
        return;
    }

    const auto cutoff = retentionCutoff(currentWatermark, *config.retentionWindowMs);
    Replay::BinaryStoreManifest retainedManifest;
    retainedManifest.segments.reserve(manifest.segments.size());
    for (const auto& segment : manifest.segments)
    {
        if (segment.getMaxWatermark() < cutoff)
        {
            continue;
        }
        retainedManifest.segments.push_back(segment);
    }

    if (retainedManifest.segments.size() == manifest.segments.size())
    {
        return;
    }

    ::fsync(fd);
    rewriteRecording(manifest, retainedManifest);
    writesSinceSync = 0;
}

void StoreOperatorHandler::rewriteRecording(
    const Replay::BinaryStoreManifest& originalManifest, const Replay::BinaryStoreManifest& retainedManifest)
{
    PRECONDITION(!originalManifest.segments.empty(), "Expected at least one segment before compaction");

    const auto manifestPath = Replay::getRecordingManifestPath(config.filePath);
    const auto temporaryFilePath = temporaryCompactionPath(config.filePath);
    const auto temporaryManifestPath = temporaryCompactionPath(manifestPath);

    std::error_code errorCode;
    std::filesystem::remove(temporaryFilePath, errorCode);
    errorCode.clear();
    std::filesystem::remove(temporaryManifestPath, errorCode);
    errorCode.clear();

    try
    {
        std::ifstream input(config.filePath, std::ios::binary);
        if (!input)
        {
            throw CannotOpenSink("Could not open recording for compaction: {}", config.filePath);
        }

        const auto dataStartOffset = originalManifest.segments.front().payloadOffset;
        std::ofstream output(temporaryFilePath, std::ios::binary | std::ios::trunc);
        if (!output)
        {
            throw CannotOpenSink("Could not create compacted recording {}", temporaryFilePath);
        }

        copyFileRange(input, output, 0, dataStartOffset);

        Replay::BinaryStoreManifest compactedManifest;
        compactedManifest.segments.reserve(retainedManifest.segments.size());
        auto nextPayloadOffset = dataStartOffset;
        for (const auto& segment : retainedManifest.segments)
        {
            copyFileRange(input, output, segment.payloadOffset, segment.storedSizeBytes);

            auto compactedSegment = segment;
            compactedSegment.payloadOffset = nextPayloadOffset;
            compactedManifest.segments.push_back(compactedSegment);
            nextPayloadOffset += segment.storedSizeBytes;
        }

        output.close();
        if (!output)
        {
            throw CannotOpenSink("Failed to finalize compacted recording {}", temporaryFilePath);
        }
        input.close();

        writeManifestFile(temporaryManifestPath, compactedManifest);

        if (fd >= 0)
        {
            ::close(fd);
            fd = -1;
        }

        std::filesystem::rename(temporaryFilePath, config.filePath);
        std::filesystem::rename(temporaryManifestPath, manifestPath);
        openFile(false);
    }
    catch (...)
    {
        if (fd < 0)
        {
            openFile(false);
        }
        std::filesystem::remove(temporaryFilePath, errorCode);
        errorCode.clear();
        std::filesystem::remove(temporaryManifestPath, errorCode);
        throw;
    }
}

void StoreOperatorHandler::initializeManifestState(const bool truncateManifest)
{
    const auto manifestPath = Replay::getRecordingManifestPath(config.filePath);
    if (truncateManifest)
    {
        std::ofstream manifestOutput(manifestPath, std::ios::trunc);
        if (!manifestOutput)
        {
            throw CannotOpenSink("Could not create binary store manifest {}", manifestPath);
        }
        manifestOutput << Replay::BINARY_STORE_MANIFEST_MAGIC << '\n';
        if (!manifestOutput)
        {
            throw CannotOpenSink("Failed to initialize binary store manifest {}", manifestPath);
        }
        nextSegmentId = 0;
        resetCurrentSegment();
        return;
    }

    if (!std::filesystem::exists(manifestPath))
    {
        std::ofstream manifestOutput(manifestPath);
        if (!manifestOutput)
        {
            throw CannotOpenSink("Could not create binary store manifest {}", manifestPath);
        }
        manifestOutput << Replay::BINARY_STORE_MANIFEST_MAGIC << '\n';
        if (!manifestOutput)
        {
            throw CannotOpenSink("Failed to initialize binary store manifest {}", manifestPath);
        }
        nextSegmentId = 0;
        resetCurrentSegment();
        return;
    }

    const auto manifest = Replay::readBinaryStoreManifest(config.filePath);
    nextSegmentId = manifest.segments.empty() ? 0 : manifest.segments.back().segmentId + 1;
    resetCurrentSegment();
}

void StoreOperatorHandler::openFile(const bool truncateExisting)
{
    if (const auto parent = std::filesystem::path(config.filePath).parent_path(); !parent.empty())
    {
        std::error_code ec;
        std::filesystem::create_directories(parent, ec);
        if (ec)
        {
            throw CannotOpenSink("Could not create output directory for {}: {}", config.filePath, ec.message());
        }
    }

    int flags = O_CREAT | O_WRONLY;
    if (truncateExisting)
    {
        flags |= O_TRUNC;
    }
#ifdef O_DSYNC
    /// leave to fdatasync interval instead of O_DSYNC for throughput
#endif
#ifdef O_DIRECT
    if (config.directIO)
    {
        flags |= O_DIRECT;
    }
#endif
    fd = ::open(config.filePath.c_str(), flags, 0644);
    if (fd < 0)
    {
        throw CannotOpenSink("Could not open output file: {} (errno={}, msg={})", config.filePath, errno, std::strerror(errno));
    }
    struct stat st{};
    if (::fstat(fd, &st) != 0)
    {
        const int err = errno;
        ::close(fd);
        fd = -1;
        throw CannotOpenSink("fstat failed for {}: errno={}, msg={}", config.filePath, err, std::strerror(err));
    }
    tail.store(static_cast<uint64_t>(st.st_size), std::memory_order_relaxed);
    headerWritten.store(st.st_size > 0, std::memory_order_relaxed);
    if (st.st_size > 0 && config.header)
    {
        validateExistingFile();
    }
}

void StoreOperatorHandler::validateExistingFile() const
{
    std::ifstream input(config.filePath, std::ios::binary);
    if (!input)
    {
        throw CannotOpenSink("Could not open existing output file for validation: {}", config.filePath);
    }

    const auto header = parseStoreHeader(input);
    const auto expectedVersion = versionForCodec(config.compression);
    const auto expectedFlags = Replay::binaryStoreFlagsForCodec(config.compression);
    const auto expectedFingerprint = fnv1a64(config.schemaText.c_str(), config.schemaText.size());
    if (header.version != expectedVersion)
    {
        throw CannotOpenSink(
            "Existing binary store format version {} is incompatible with requested version {} for {}",
            header.version,
            expectedVersion,
            config.filePath);
    }
    if (header.flags != expectedFlags || header.compression != config.compression)
    {
        throw CannotOpenSink(
            "Existing binary store codec flags {} are incompatible with requested flags {} for {}",
            header.flags,
            expectedFlags,
            config.filePath);
    }
    if (header.schemaFingerprint != expectedFingerprint)
    {
        throw CannotOpenSink("Existing binary store schema fingerprint does not match {}", config.filePath);
    }
}

uint64_t StoreOperatorHandler::writeAtTail(const uint8_t* data, size_t len)
{
    const uint64_t off = tail.fetch_add(len, std::memory_order_relaxed);
    const ssize_t written = ::pwrite(fd, data, len, static_cast<off_t>(off));
    if (written < 0 || static_cast<size_t>(written) != len)
    {
        throw CannotOpenSink("pwrite failed: errno={} {}", errno, std::strerror(errno));
    }
    if (config.fdatasyncInterval > 0)
    {
        ++writesSinceSync;
        if (writesSinceSync >= config.fdatasyncInterval)
        {
            ::fdatasync(fd);
            writesSinceSync = 0;
        }
    }
    return off;
}

void StoreOperatorHandler::writeHeaderIfNeeded()
{
    bool expected = false;
    if (!headerWritten.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
    {
        return;
    }

    if (tail.load(std::memory_order_relaxed) > 0)
    {
        return;
    }

    const auto fingerprint = fnv1a64(config.schemaText.c_str(), config.schemaText.size());
    const auto schemaLen = static_cast<uint32_t>(config.schemaText.size());
    const auto version = versionForCodec(config.compression);
    const auto flags = Replay::binaryStoreFlagsForCodec(config.compression);

    std::vector<uint8_t> buf(static_cast<size_t>(Replay::binaryStoreHeaderSize(schemaLen)));
    size_t off = 0;
    std::memcpy(buf.data() + off, Replay::BINARY_STORE_MAGIC.data(), Replay::BINARY_STORE_MAGIC.size());
    off += Replay::BINARY_STORE_MAGIC.size();
    std::memcpy(buf.data() + off, &version, sizeof(version));
    off += sizeof(version);
    std::memcpy(buf.data() + off, &Replay::BINARY_STORE_ENDIANNESS_LITTLE, sizeof(Replay::BINARY_STORE_ENDIANNESS_LITTLE));
    off += sizeof(Replay::BINARY_STORE_ENDIANNESS_LITTLE);
    std::memcpy(buf.data() + off, &flags, sizeof(flags));
    off += sizeof(flags);
    std::memcpy(buf.data() + off, &fingerprint, sizeof(fingerprint));
    off += sizeof(fingerprint);
    std::memcpy(buf.data() + off, &schemaLen, sizeof(schemaLen));
    off += sizeof(schemaLen);
    std::memcpy(buf.data() + off, config.schemaText.data(), schemaLen);

    const auto headerSize = static_cast<uint64_t>(buf.size());
    const uint64_t off0 = tail.fetch_add(headerSize, std::memory_order_relaxed);
    if (off0 != 0)
    {
        return;
    }
    const ssize_t written = ::pwrite(fd, buf.data(), buf.size(), 0);
    if (written < 0 || static_cast<size_t>(written) != buf.size())
    {
        throw CannotOpenSink("Writing store header failed: errno={} {}", errno, std::strerror(errno));
    }
}

uint64_t StoreOperatorHandler::fnv1a64(const char* data, size_t len)
{
    uint64_t hash = FNVOffsetBasis;
    for (size_t i = 0; i < len; ++i)
    {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= FNVPrime;
    }
    return hash;
}

}
