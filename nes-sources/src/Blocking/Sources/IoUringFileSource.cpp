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

#include <Blocking/Sources/IoUringFileSource.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <liburing.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceUtility.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

namespace
{
/// O_DIRECT alignment: the device logical block size (512 on this NVMe). offset, length, and the
/// destination buffer must all be multiples of this.
constexpr std::size_t DIRECT_IO_ALIGN = 512;
}

IoUringFileSource::IoUringFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersIoUring::FILEPATH))
{
    if (const char* const env = std::getenv("NES_IOURING_QD"))
    {
        queueDepth = static_cast<unsigned>(std::max<unsigned long>(1, std::strtoul(env, nullptr, 10)));
    }
    if (const char* const env = std::getenv("NES_IOURING_DIRECT"))
    {
        directIo = std::string_view(env) != "0";
    }
}

IoUringFileSource::~IoUringFileSource()
{
    close();
}

void IoUringFileSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    bufferSize = bufferProvider->getBufferSize();

    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    if (not realPath)
    {
        throw CannotOpenSource("IoUringFileSource: could not resolve path: {} - {}", filePath, getErrorMessageFromERRNO());
    }
    fileDescriptor = ::open(realPath.get(), O_RDONLY | (directIo ? O_DIRECT : 0));
    if (fileDescriptor == -1)
    {
        throw CannotOpenSource("IoUringFileSource: failed to open file: {} - {}", filePath, getErrorMessageFromERRNO());
    }

    struct stat st{};
    if (::fstat(fileDescriptor, &st) != 0)
    {
        throw CannotOpenSource("IoUringFileSource: fstat failed for {} - {}", filePath, getErrorMessageFromERRNO());
    }
    fileSize = static_cast<std::size_t>(st.st_size);
    numReads = bufferSize == 0 ? 0 : (fileSize + bufferSize - 1) / bufferSize;

    if (const int rc = io_uring_queue_init(queueDepth, &ring, 0); rc < 0)
    {
        throw CannotOpenSource("IoUringFileSource: io_uring_queue_init(QD={}) failed: {}", queueDepth, std::strerror(-rc));
    }
    ringInitialized = true;
    slots.assign(queueDepth, Slot{});

    /// Prime the pipeline: submit up to QD reads so QD completions are always in flight.
    const std::uint64_t initial = std::min<std::uint64_t>(queueDepth, numReads);
    for (std::uint64_t seq = 0; seq < initial; ++seq)
    {
        submitRead(seq);
    }
    nextSubmitSeq = initial;
    if (initial > 0)
    {
        io_uring_submit(&ring);
    }
}

void IoUringFileSource::submitRead(const std::uint64_t seq)
{
    Slot& slot = slots[seq % queueDepth];
    slot.buffer = bufferProvider->getBufferBlocking();
    slot.length = 0;
    slot.ready = false;
    /// Stamp the read-submit time (us): the deep-QD analog of the non-prefill path's read-start
    /// stamp. The MonitoringSink (and the e2e latency it reports) ONLY counts buffers whose
    /// sourceCreationTimestamp != INITIAL_VALUE -- without this the prefill path's buffers are all
    /// dropped, latencies stay empty, and the sink writes nothing.
    slot.submitTimeMicros = ingestNowMicros();

    const std::size_t offset = seq * bufferSize;
    std::size_t length = std::min(bufferSize, fileSize - offset);
    char* const dest = slot.buffer.getAvailableMemoryArea<char>().data();
    if (directIo)
    {
        /// O_DIRECT requires offset, length, and buffer all aligned to the 512-byte device block.
        /// offset (seq*128KiB) is already aligned; round the partial tail length UP to 512 (the read
        /// short-returns the real file bytes via cqe->res). The buffer must be 512-aligned -- engine
        /// pool buffers (128 KiB stride from a page-aligned slab) are, but guard to fail loud if not.
        length = (length + DIRECT_IO_ALIGN - 1) & ~(DIRECT_IO_ALIGN - 1);
        INVARIANT(
            (reinterpret_cast<std::uintptr_t>(dest) % DIRECT_IO_ALIGN) == 0,
            "O_DIRECT needs {}-byte aligned buffers but pool buffer is not aligned; use a private aligned pool",
            DIRECT_IO_ALIGN);
    }
    io_uring_sqe* const sqe = io_uring_get_sqe(&ring);
    INVARIANT(sqe != nullptr, "io_uring submission queue unexpectedly full (QD={})", queueDepth);
    io_uring_prep_read(sqe, fileDescriptor, dest, static_cast<unsigned>(length), offset);
    io_uring_sqe_set_data64(sqe, seq);
}

std::optional<TupleBuffer> IoUringFileSource::takePreFilledBuffer()
{
    if (headSeq >= numReads)
    {
        return std::nullopt; /// every read for this pass has been emitted -> end of stream
    }

    Slot& head = slots[headSeq % queueDepth];
    /// Wait until the head (oldest) read has completed. Completions may arrive out of order, so we drain
    /// CQEs, stash each slot's result, and only return once the in-order head is ready.
    while (not head.ready)
    {
        io_uring_cqe* cqe = nullptr;
        if (const int rc = io_uring_wait_cqe(&ring, &cqe); rc < 0)
        {
            throw RunningRoutineFailure("IoUringFileSource: io_uring_wait_cqe failed: {}", std::strerror(-rc));
        }
        const std::uint64_t seq = io_uring_cqe_get_data64(cqe);
        const int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);
        if (res < 0)
        {
            throw RunningRoutineFailure("IoUringFileSource: read for offset {} failed: {}", seq * bufferSize, std::strerror(-res));
        }
        Slot& slot = slots[seq % queueDepth];
        slot.length = static_cast<std::size_t>(res);
        slot.ready = true;
    }

    TupleBuffer buffer = std::move(head.buffer);
    buffer.setNumberOfTuples(head.length);
    buffer.setSourceCreationTimestampInMS(Timestamp(head.submitTimeMicros));
    head.ready = false;

    /// Refill: keep the ring at QD by submitting the next outstanding read into the slot we just freed.
    if (nextSubmitSeq < numReads)
    {
        submitRead(nextSubmitSeq);
        io_uring_submit(&ring);
        ++nextSubmitSeq;
    }
    ++headSeq;
    return buffer;
}

BlockingSource::FillTupleBufferResult IoUringFileSource::fillTupleBuffer(TupleBuffer&, const std::stop_token&, std::size_t)
{
    /// Unreachable: preFillsBuffers() == true, so the runner uses takePreFilledBuffer() exclusively.
    return BlockingSource::FillTupleBufferResult::eos();
}

void IoUringFileSource::close()
{
    if (ringInitialized)
    {
        io_uring_queue_exit(&ring);
        ringInitialized = false;
    }
    slots.clear(); /// release any pool buffers still held by in-flight slots
    if (fileDescriptor != -1)
    {
        ::close(fileDescriptor);
        fileDescriptor = -1;
    }
}

DescriptorConfig::Config IoUringFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersIoUring>(std::move(config), NAME);
}

std::ostream& IoUringFileSource::toString(std::ostream& str) const
{
    str << std::format("\nIoUringFileSource(filepath: {}, queueDepth: {}, fileSize: {})", filePath, queueDepth, fileSize);
    return str;
}

SourceValidationRegistryReturnType RegisterIoUringFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return IoUringFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterIoUringFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<IoUringFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterIoUringFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string("file_path")))
    {
        throw InvalidConfigParameter("Mock IoUringFileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(
        std::string("file_path"), systestAdaptorArguments.testFilePath.string());

    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterIoUringFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string("file_path")))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        std::string("file_path"), systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
