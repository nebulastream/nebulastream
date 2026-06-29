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

IoUringFileSource::IoUringFileSource(const SourceDescriptor& sourceDescriptor)
    : filePath(sourceDescriptor.getFromConfig(ConfigParametersIoUring::FILEPATH))
{
    if (const char* const env = std::getenv("NES_IOURING_QD"))
    {
        queueDepth = static_cast<unsigned>(std::max<unsigned long>(1, std::strtoul(env, nullptr, 10)));
        autoQueueDepth = false;
    }
    if (const char* const env = std::getenv("NES_IOURING_DIRECT"))
    {
        directIo = std::string_view(env) != "0";
    }
    if (const char* const env = std::getenv("NES_IOURING_REGBUF"))
    {
        regBuf = std::string_view(env) != "0";
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

    /// Derive the ring depth from the (inflight-sized) pool unless NES_IOURING_QD pinned it: half the pool
    /// for the ring, half as in-processing headroom, so the source never holds more than the pool allows and
    /// can always refill. Cap at 64 (QD>=32 already saturates the device; >64 adds nothing).
    if (autoQueueDepth)
    {
        const std::size_t poolBuffers = bufferProvider->getNumOfPooledBuffers();
        queueDepth = static_cast<unsigned>(std::clamp<std::size_t>(poolBuffers / 2, 1, 64));
    }

    if (const int rc = io_uring_queue_init(queueDepth, &ring, 0); rc < 0)
    {
        throw CannotOpenSource("IoUringFileSource: io_uring_queue_init(QD={}) failed: {}", queueDepth, std::strerror(-rc));
    }
    ringInitialized = true;
    slots.assign(queueDepth, Slot{});
    cqeBatch.assign(queueDepth, nullptr);

    if (regBuf)
    {
        maybeRegisterBuffers();
    }

    /// Prime the pipeline: submit up to QD reads so QD completions are always in flight. The pool holds
    /// >= QD free buffers at open (it is inflight-sized, >= 2*QD), so these acquisitions never block; a
    /// default (never-stopping) token is fine here.
    const std::uint64_t initial = std::min<std::uint64_t>(queueDepth, numReads);
    const std::stop_token noStop;
    std::uint64_t primed = 0;
    for (std::uint64_t seq = 0; seq < initial; ++seq)
    {
        if (not submitRead(seq, noStop))
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

void IoUringFileSource::maybeRegisterBuffers()
{
    const auto slab = bufferProvider->getContiguousSlab();
    if (not slab)
    {
        NES_WARNING("IoUringFileSource: NES_IOURING_REGBUF set but the buffer provider is not slab-backed; using non-fixed reads");
        return;
    }
    /// Fixed O_DIRECT reads require device-block (512) aligned payloads: the kernel checks the read offset
    /// WITHIN the registered region, so every payload's offset must be 512-aligned. That holds iff the pool's
    /// payload alignment is >= 512 (set `global_buffer_alignment: 512`). Check the guarantee up front rather
    /// than probing one buffer (with smaller alignment only some payloads happen to align).
    if (bufferProvider->getBufferAlignment() < DIRECT_IO_ALIGN)
    {
        NES_WARNING(
            "IoUringFileSource: NES_IOURING_REGBUF needs payloads aligned to >= {} bytes but the pool guarantees "
            "{} (set global_buffer_alignment: {}); using non-fixed reads",
            DIRECT_IO_ALIGN,
            bufferProvider->getBufferAlignment(),
            DIRECT_IO_ALIGN);
        return;
    }

    /// Register the whole slab as one buffer (index 0); every payload lies inside it. Round the base up to the
    /// device block so payload offsets within the region stay aligned even if the slab base is only cache-line
    /// aligned (with global_buffer_alignment >= 512 the base is already aligned and this is a no-op).
    auto* const slabBase = slab->first;
    auto* const regBase
        = reinterpret_cast<std::uint8_t*>((reinterpret_cast<std::uintptr_t>(slabBase) + DIRECT_IO_ALIGN - 1) & ~(DIRECT_IO_ALIGN - 1));
    const std::size_t regLen = slab->second - static_cast<std::size_t>(regBase - slabBase);
    const iovec iov{.iov_base = regBase, .iov_len = regLen};
    if (const int rc = io_uring_register_buffers(&ring, &iov, 1); rc < 0)
    {
        NES_WARNING("IoUringFileSource: io_uring_register_buffers({} bytes) failed: {}; using non-fixed reads", regLen, std::strerror(-rc));
        return;
    }
    buffersRegistered = true;
}

bool IoUringFileSource::submitRead(const std::uint64_t seq, const std::stop_token& stopToken)
{
    Slot& slot = slots[seq % queueDepth];
    /// Acquire a pool buffer. With the inflight-sized private pool, this blocks when all buffers are in
    /// flight (= backpressure). Poll with a short timeout so a shutdown mid-acquire is abandoned promptly
    /// instead of stalling for the full pool timeout.
    std::optional<TupleBuffer> acquired;
    while (not acquired.has_value())
    {
        acquired = bufferProvider->getBufferWithTimeout(std::chrono::milliseconds(25));
        if (not acquired.has_value() && stopToken.stop_requested())
        {
            return false;
        }
    }
    slot.buffer = std::move(*acquired);
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
    if (buffersRegistered)
    {
        /// dest lies within the single registered region (index 0); prep_read_fixed skips the per-read
        /// get_user_pages because the pages are already pinned.
        io_uring_prep_read_fixed(sqe, fileDescriptor, dest, static_cast<unsigned>(length), offset, 0);
    }
    else
    {
        io_uring_prep_read(sqe, fileDescriptor, dest, static_cast<unsigned>(length), offset);
    }
    io_uring_sqe_set_data64(sqe, seq);
    return true;
}

std::optional<TupleBuffer> IoUringFileSource::takePreFilledBuffer(const std::stop_token& stopToken)
{
    if (headSeq >= numReads)
    {
        return std::nullopt; /// every read for this pass has been emitted -> end of stream
    }

    Slot& head = slots[headSeq % queueDepth];
    /// Wait until the head (oldest) read has completed. Completions may arrive out of order, so we drain
    /// CQEs, stash each slot's result, and only return once the in-order head is ready. Reaping is BATCHED
    /// (io_uring_peek_batch_cqe + cq_advance) so one syscall drains all currently-ready completions instead
    /// of one wait_cqe per completion; only block (wait_cqe) when nothing is ready yet.
    const auto markReady = [this](const std::uint64_t seq, const int res)
    {
        if (res < 0)
        {
            throw RunningRoutineFailure("IoUringFileSource: read for offset {} failed: {}", seq * bufferSize, std::strerror(-res));
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
        else
        {
            io_uring_cqe* cqe = nullptr;
            if (const int rc = io_uring_wait_cqe(&ring, &cqe); rc < 0)
            {
                throw RunningRoutineFailure("IoUringFileSource: io_uring_wait_cqe failed: {}", std::strerror(-rc));
            }
            const std::uint64_t seq = io_uring_cqe_get_data64(cqe);
            const int res = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            markReady(seq, res);
        }
    }

    TupleBuffer buffer = std::move(head.buffer);
    buffer.setNumberOfTuples(head.length);
    buffer.setSourceCreationTimestampInMS(Timestamp(head.submitTimeMicros));
    head.ready = false;

    /// Refill: keep the ring at QD by submitting the next outstanding read into the slot we just freed. If the
    /// acquire was abandoned because a stop was requested, skip the refill -- we still return the head buffer
    /// we already reaped; the runner exits at its next stop check.
    if (nextSubmitSeq < numReads)
    {
        if (submitRead(nextSubmitSeq, stopToken))
        {
            io_uring_submit(&ring);
            ++nextSubmitSeq;
        }
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
    str << std::format(
        "\nIoUringFileSource(filepath: {}, queueDepth: {}, fileSize: {}, directIo: {}, registeredBuffers: {})",
        filePath,
        queueDepth,
        fileSize,
        directIo,
        buffersRegistered);
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
