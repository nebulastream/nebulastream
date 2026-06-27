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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <liburing.h>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// IoUringFileSource reads a regular file with a DEEP queue depth from a SINGLE io thread using Linux
/// io_uring -- the cold-read lever (HANDOVER 2_engine_to_e2e §20 / §3 rung-1).
///
/// Why: the async FileSource is QD1 -- it issues ONE async_read at a time on an asio posix descriptor,
/// but a regular file is always epoll-"ready" so asio falls back to a BLOCKING read = queue depth 1. On
/// COLD data that is disk-latency-bound (~2.4 GB/s, ~34% of the ~6.9 GB/s NVMe cap). io_uring keeps many
/// reads in flight from ONE thread (QD>=16 saturates this drive, §2), hiding the per-read latency.
///
/// How (zero-copy, no runner/interface change): this is a BlockingSource using the existing PREFILL hook.
/// open() inits an io_uring ring and submits QD reads DIRECTLY into pool buffers (acquired from the engine
/// buffer provider) at contiguous file offsets. takePreFilledBuffer() reaps the OLDEST read in file order
/// (completions can arrive out of order; we stash readiness and only emit in order, required because the
/// CSV formatter stitches partial tuples across consecutive buffers by sequence number), then refills the
/// ring to keep QD full and hands the completed pool buffer straight to the engine -- no extra copy.
///
/// SINGLE-PASS (cold reads are single-pass by nature). Env tunables: NES_IOURING_QD (default 16) and
/// NES_IOURING_DIRECT=1 for O_DIRECT -- which bypasses the page cache AND readahead so deep QD reaches the
/// device (buffered QD is readahead-bound: cold ~2.2 GB/s at any QD; O_DIRECT QD16 ~5.3 GB/s, 2.5x). O_DIRECT
/// needs 512-byte (device logical block) alignment of offset/length/buffer -- the partial tail is rounded up
/// and the pool buffer alignment is asserted. Follow-ons: registered fixed buffers, multi-pass. Keep the async
/// `File` source as the A/B cold baseline.
class IoUringFileSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "IoUringFile";

    explicit IoUringFileSource(const SourceDescriptor& sourceDescriptor);
    ~IoUringFileSource() override;

    IoUringFileSource(const IoUringFileSource&) = delete;
    IoUringFileSource& operator=(const IoUringFileSource&) = delete;
    IoUringFileSource(IoUringFileSource&&) = delete;
    IoUringFileSource& operator=(IoUringFileSource&&) = delete;

    /// Not used: preFillsBuffers() is true, so the runner drains takePreFilledBuffer() instead.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// Open the file, init the io_uring ring, and submit the initial QD reads into pool buffers.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Tear down the ring and close the file.
    void close() override;

    [[nodiscard]] bool preFillsBuffers() const override { return true; }

    /// Reap the next read in file order, refill the ring, and return the filled buffer (nullopt = EoS).
    [[nodiscard]] std::optional<TupleBuffer> takePreFilledBuffer() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Acquire a pool buffer and queue a read for read `seq` at offset seq*bufferSize into slot seq%queueDepth.
    void submitRead(std::uint64_t seq);

    std::string filePath;
    int fileDescriptor = -1;
    std::size_t fileSize = 0;
    std::size_t bufferSize = 0;
    std::size_t numReads = 0; ///< ceil(fileSize / bufferSize) -- total reads for one pass
    unsigned queueDepth = 16;
    /// O_DIRECT (env NES_IOURING_DIRECT=1): bypass the page cache + readahead so each read is an
    /// independent device request -> deep QD reaches the device (buffered QD is readahead-bound).
    /// Requires 512-byte (device logical block) alignment of offset, length, and buffer.
    bool directIo = false;
    bool ringInitialized = false;

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    io_uring ring{};

    /// One slot per in-flight read, indexed by seq % queueDepth. Holds the pool buffer the kernel reads
    /// into (kept alive until reaped) and the completion state for in-order emission.
    struct Slot
    {
        TupleBuffer buffer; ///< pool buffer the read targets; held until the completion is reaped
        std::size_t length = 0; ///< bytes read (cqe->res), valid once `ready`
        std::uint64_t submitTimeMicros = 0; ///< read-submit time (us); stamped onto the buffer as sourceCreationTimestamp
        bool ready = false; ///< completion seen for this slot
    };

    std::vector<Slot> slots;

    std::uint64_t headSeq = 0; ///< next read (in file order) to emit
    std::uint64_t nextSubmitSeq = 0; ///< next read for which we still owe a submission
};

struct ConfigParametersIoUring
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string("file_path"),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
