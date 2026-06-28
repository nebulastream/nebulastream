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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/asio/awaitable.hpp>

#include <Async/Sources/IoUringAsyncSource.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// IoUringAsyncFileSource is the ASYNC (asio io_context) counterpart of the blocking IoUringFileSource: a
/// single-thread, deep-queue-depth io_uring file reader for cold reads. It reads QD buffers in flight at
/// contiguous file offsets directly into pool buffers (zero extra copy) and emits them in file order.
///
/// Difference vs the blocking variant: it does NOT own a dedicated thread. It runs on the shared async
/// io_context and, instead of blocking on io_uring_wait_cqe, suspends via the base's eventfd bridge
/// (co_await waitForCompletion()) so other async sources keep running while it waits on the disk. The goal is
/// to match the blocking variant's cold-read throughput while staying on the production async path.
///
/// Env tunables (shared with the blocking variant): NES_IOURING_QD (ring depth), NES_IOURING_DIRECT=1
/// (O_DIRECT -- bypass page cache + readahead so deep QD reaches the device), NES_IOURING_REGBUF=1 (register
/// the pool slab for fixed-buffer O_DIRECT reads; needs global_buffer_alignment: 512). SINGLE-PASS.
class IoUringAsyncFileSource final : public IoUringAsyncSource
{
public:
    static constexpr std::string_view NAME = "IoUringFileAsync";

    explicit IoUringAsyncFileSource(const SourceDescriptor& sourceDescriptor);

    IoUringAsyncFileSource(const IoUringAsyncFileSource&) = delete;
    IoUringAsyncFileSource& operator=(const IoUringAsyncFileSource&) = delete;
    IoUringAsyncFileSource(IoUringAsyncFileSource&&) = delete;
    IoUringAsyncFileSource& operator=(IoUringAsyncFileSource&&) = delete;

    /// Reap the next read in file order (refilling the ring), or nullopt at end-of-stream / on cancellation.
    asio::awaitable<std::optional<TupleBuffer>, Executor> takePreFilledBuffer() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

protected:
    asio::awaitable<void, Executor> onOpen() override;
    void onClose() override;

private:
    /// Acquire a pool buffer and queue a read for read `seq` (offset seq*bufferSize) into slot seq%queueDepth.
    /// Returns false if it gave up acquiring because a stop was requested (no read queued).
    asio::awaitable<bool, Executor> submitRead(std::uint64_t seq);
    /// Best-effort: register the pool's contiguous slab for fixed-buffer O_DIRECT reads (sets buffersRegistered).
    void maybeRegisterBuffers();

    std::string filePath;
    int fileDescriptor = -1;
    std::size_t fileSize = 0;
    std::size_t numReads = 0; ///< ceil(fileSize / bufferSize) -- total reads for one pass

    bool directIo = false; ///< NES_IOURING_DIRECT=1: O_DIRECT (bypass page cache + readahead)
    bool regBuf = false; ///< NES_IOURING_REGBUF=1: register the pool slab for prep_read_fixed
    bool buffersRegistered = false;

    /// One slot per in-flight read, indexed by seq % queueDepth.
    struct Slot
    {
        TupleBuffer buffer; ///< pool buffer the read targets; held until the completion is reaped
        std::size_t length = 0; ///< bytes read (cqe->res), valid once `ready`
        std::uint64_t submitTimeMicros = 0; ///< read-submit time (us); stamped as sourceCreationTimestamp
        bool ready = false; ///< completion seen for this slot
    };
    std::vector<Slot> slots;

    std::uint64_t headSeq = 0; ///< next read (in file order) to emit
    std::uint64_t nextSubmitSeq = 0; ///< next read for which we still owe a submission
};

struct ConfigParametersIoUringAsyncFile
{
    static inline const DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        std::string("file_path"),
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, FILEPATH);
};

}
