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

#include <Async/Sources/IoUringAsyncFileSource.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>

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
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <liburing.h>

#include <boost/asio/awaitable.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceUtility.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
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

IoUringAsyncFileSource::IoUringAsyncFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersIoUringAsyncFile::FILEPATH))
{
    if (const char* const env = std::getenv("NES_IOURING_DIRECT"))
    {
        directIo = std::string_view(env) != "0";
    }
    if (const char* const env = std::getenv("NES_IOURING_REGBUF"))
    {
        regBuf = std::string_view(env) != "0";
    }
}

asio::awaitable<void, Executor> IoUringAsyncFileSource::onOpen()
{
    const auto realPath = std::unique_ptr<char, decltype(std::free)*>{realpath(filePath.c_str(), nullptr), std::free};
    if (not realPath)
    {
        throw CannotOpenSource("IoUringAsyncFileSource: could not resolve path: {} - {}", filePath, getErrorMessageFromERRNO());
    }
    fileDescriptor = ::open(realPath.get(), O_RDONLY | (directIo ? O_DIRECT : 0));
    if (fileDescriptor == -1)
    {
        throw CannotOpenSource("IoUringAsyncFileSource: failed to open file: {} - {}", filePath, getErrorMessageFromERRNO());
    }

    struct stat st{};
    if (::fstat(fileDescriptor, &st) != 0)
    {
        throw CannotOpenSource("IoUringAsyncFileSource: fstat failed for {} - {}", filePath, getErrorMessageFromERRNO());
    }
    fileSize = static_cast<std::size_t>(st.st_size);
    numReads = bufferSize == 0 ? 0 : (fileSize + bufferSize - 1) / bufferSize;

    co_await initRing();
    slots.assign(queueDepth, Slot{});

    if (regBuf)
    {
        maybeRegisterBuffers();
    }

    /// Prime the pipeline: submit up to QD reads so QD completions are always in flight.
    const std::uint64_t initial = std::min<std::uint64_t>(queueDepth, numReads);
    std::uint64_t primed = 0;
    for (std::uint64_t seq = 0; seq < initial; ++seq)
    {
        if (not co_await submitRead(seq))
        {
            break;
        }
        ++primed;
    }
    nextSubmitSeq = primed;
    if (primed > 0)
    {
        io_uring_submit(&ring);
    }
}

void IoUringAsyncFileSource::maybeRegisterBuffers()
{
    const auto slab = bufferProvider->getContiguousSlab();
    if (not slab)
    {
        NES_WARNING("IoUringAsyncFileSource: NES_IOURING_REGBUF set but the buffer provider is not slab-backed; using non-fixed reads");
        return;
    }
    if (bufferProvider->getBufferAlignment() < DIRECT_IO_ALIGN)
    {
        NES_WARNING(
            "IoUringAsyncFileSource: NES_IOURING_REGBUF needs payloads aligned to >= {} bytes but the pool guarantees "
            "{} (set global_buffer_alignment: {}); using non-fixed reads",
            DIRECT_IO_ALIGN,
            bufferProvider->getBufferAlignment(),
            DIRECT_IO_ALIGN);
        return;
    }

    auto* const slabBase = slab->first;
    auto* const regBase
        = reinterpret_cast<std::uint8_t*>((reinterpret_cast<std::uintptr_t>(slabBase) + DIRECT_IO_ALIGN - 1) & ~(DIRECT_IO_ALIGN - 1));
    const std::size_t regLen = slab->second - static_cast<std::size_t>(regBase - slabBase);
    const iovec iov{.iov_base = regBase, .iov_len = regLen};
    if (const int rc = io_uring_register_buffers(&ring, &iov, 1); rc < 0)
    {
        NES_WARNING(
            "IoUringAsyncFileSource: io_uring_register_buffers({} bytes) failed: {}; using non-fixed reads", regLen, std::strerror(-rc));
        return;
    }
    buffersRegistered = true;
}

asio::awaitable<bool, Executor> IoUringAsyncFileSource::submitRead(const std::uint64_t seq)
{
    auto acquired = co_await acquireBuffer();
    if (not acquired.has_value())
    {
        co_return false; /// stop requested mid-acquire
    }
    Slot& slot = slots[seq % queueDepth];
    slot.buffer = std::move(*acquired);
    slot.length = 0;
    slot.ready = false;
    /// Stamp the read-submit time (us) as sourceCreationTimestamp so the sink's e2e latency/throughput include
    /// the read; the MonitoringSink drops buffers whose stamp is still INITIAL_VALUE.
    slot.submitTimeMicros = ingestNowMicros();

    const std::size_t offset = seq * bufferSize;
    std::size_t length = std::min(bufferSize, fileSize - offset);
    char* const dest = slot.buffer.getAvailableMemoryArea<char>().data();
    if (directIo)
    {
        /// O_DIRECT requires offset, length, and buffer all 512-aligned. offset (seq*bufferSize) is aligned;
        /// round the partial tail length UP (the read short-returns the real bytes via cqe->res).
        length = (length + DIRECT_IO_ALIGN - 1) & ~(DIRECT_IO_ALIGN - 1);
        INVARIANT(
            (reinterpret_cast<std::uintptr_t>(dest) % DIRECT_IO_ALIGN) == 0,
            "O_DIRECT needs {}-byte aligned buffers but pool buffer is not aligned; use a private aligned pool",
            DIRECT_IO_ALIGN);
    }
    io_uring_sqe* const sqe = io_uring_get_sqe(&ring);
    INVARIANT(sqe != nullptr, "io_uring submission queue unexpectedly full (QD={})", queueDepth);
    if (buffersRegistered)
    {
        io_uring_prep_read_fixed(sqe, fileDescriptor, dest, static_cast<unsigned>(length), offset, 0);
    }
    else
    {
        io_uring_prep_read(sqe, fileDescriptor, dest, static_cast<unsigned>(length), offset);
    }
    io_uring_sqe_set_data64(sqe, seq);
    co_return true;
}

asio::awaitable<std::optional<TupleBuffer>, Executor> IoUringAsyncFileSource::takePreFilledBuffer()
{
    if (headSeq >= numReads)
    {
        co_return std::nullopt; /// every read for this pass has been emitted -> end of stream
    }

    Slot& head = slots[headSeq % queueDepth];
    /// Wait until the head (oldest) read has completed. Completions may arrive out of order, so we drain CQEs,
    /// stash each slot's result, and only return once the in-order head is ready. We suspend on the eventfd
    /// bridge (not io_uring_wait_cqe) so the io_context thread stays free for sibling sources.
    const auto markReady = [this](const std::uint64_t seq, const int res)
    {
        if (res < 0)
        {
            throw RunningRoutineFailure("IoUringAsyncFileSource: read for offset {} failed: {}", seq * bufferSize, std::strerror(-res));
        }
        Slot& slot = slots[seq % queueDepth];
        slot.length = static_cast<std::size_t>(res);
        slot.ready = true;
    };
    while (not head.ready)
    {
        if (const unsigned n = io_uring_peek_batch_cqe(&ring, cqeBatch.data(), static_cast<unsigned>(cqeBatch.size())); n > 0)
        {
            for (unsigned i = 0; i < n; ++i)
            {
                markReady(io_uring_cqe_get_data64(cqeBatch[i]), cqeBatch[i]->res);
            }
            io_uring_cq_advance(&ring, n);
        }
        else if (not co_await waitForCompletion())
        {
            co_return std::nullopt; /// cancelled (query stop)
        }
    }

    TupleBuffer buffer = std::move(head.buffer);
    buffer.setNumberOfTuples(head.length);
    buffer.setSourceCreationTimestampInMS(Timestamp(head.submitTimeMicros));
    head.ready = false;

    /// Refill: keep the ring at QD by submitting the next outstanding read into the slot we just freed.
    if (nextSubmitSeq < numReads)
    {
        if (co_await submitRead(nextSubmitSeq))
        {
            io_uring_submit(&ring);
            ++nextSubmitSeq;
        }
    }
    ++headSeq;
    co_return buffer;
}

void IoUringAsyncFileSource::onClose()
{
    slots.clear(); /// release any pool buffers still held by in-flight slots
    if (fileDescriptor != -1)
    {
        ::close(fileDescriptor);
        fileDescriptor = -1;
    }
}

DescriptorConfig::Config IoUringAsyncFileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersIoUringAsyncFile>(std::move(config), std::string(NAME));
}

std::ostream& IoUringAsyncFileSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nIoUringAsyncFileSource(filepath: {}, queueDepth: {}, fileSize: {}, directIo: {}, registeredBuffers: {})",
        filePath,
        queueDepth,
        fileSize,
        directIo,
        buffersRegistered);
    return str;
}

SourceValidationRegistryReturnType RegisterIoUringFileAsyncSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return IoUringAsyncFileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterIoUringFileAsyncSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<IoUringAsyncFileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterIoUringFileAsyncInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(std::string("file_path")))
    {
        throw InvalidConfigParameter("Mock IoUringAsyncFileSource cannot use given inline data if a 'file_path' is set");
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

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterIoUringFileAsyncFileData(FileDataRegistryArguments systestAdaptorArguments)
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
