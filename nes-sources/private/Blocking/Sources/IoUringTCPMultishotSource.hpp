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
#include <deque>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <liburing.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/// IoUringTCPMultishotSource (registered as "IoUringTCPMultishot") -- the SOTA io_uring TCP recv path.
///
/// This is the principled successor to the two earlier io_uring TCP sources, motivated by profiling
/// (see HANDOVER §48): the asio TCP producer is recv-COPY-bound (~60% `_copy_to_iter`, only 2.7% asio
/// overhead), so on a loopback / kernel-6.8 stream io_uring cannot beat asio on copy -- the realistic
/// target is PARITY. The earlier blocking io_uring source LOST to asio (~3.5 vs ~5.3 GB/s) not because of
/// the copy but because its single-shot offset-fill recv issued one `io_uring_enter` per recv (one syscall
/// per ~64 KiB loopback segment), whereas asio's `async_read` drains a whole 128 KiB buffer per resume.
///
/// The lever this source pulls is MULTISHOT recv + a PROVIDED BUFFER RING (`IORING_REGISTER_PBUF_RING`):
/// one armed `recv_multishot` makes the kernel post a completion per arriving segment WITHOUT a per-recv
/// submission, so a single `io_uring_submit_and_wait_timeout` reaps a BATCH of completions -- removing the
/// per-recv enter that sank the offset-fill variant. The socket fd is registered (fixed file index 0) to
/// skip the per-op fd lookup, and the ring is driven DIRECTLY on this source's own dedicated thread (a
/// BlockingSource), so there is no eventfd->epoll bridge (the async source's fatal flaw, HANDOVER §48.3).
///
/// Buffer model (no extra staging copy): the provided-ring entries are backed by this source's own
/// private-pool TupleBuffers. A completion hands back the buffer id it filled and the byte count; we emit
/// THAT pool buffer directly (numberOfTuples = bytes recv'd) and replenish the consumed ring slot with a
/// fresh pool buffer. Under burst load most recvs fill a whole buffer; a short segment yields a smaller
/// buffer, which the CSV formatter stitches across (buffers are emitted strictly in byte order, since a
/// single multishot on a single socket completes in arrival order). In-flight is bounded by the ring size
/// plus the private pool, and `getBufferBlocking` on replenish is the backpressure.
///
/// STATUS (documented negative result -- see HANDOVER 2_engine_to_e2e §48/§49): on a loopback / kernel-6.8
/// stream this LOSES to the asio TCPSource (~4.1 vs ~5.1 GB/s e2e). Profiling shows the asio producer is
/// recv-COPY-bound (~60% `_copy_to_iter`) with only ~2.7% asio overhead, so there is no per-syscall headroom
/// for io_uring to reclaim; multishot's per-segment PARTIAL buffers (~40% more tasks than asio's full buffers)
/// only add cost, and true zero-copy RX (zcrx) needs kernel >=6.15 + a real NIC (absent on loopback). KNOWN
/// LIMITATION: under sustained high-volume single-query streaming the reap/replenish loop can STALL (recv
/// starvation / backpressure) -- correct for bounded inputs (systests pass) but not yet production-robust.
/// Kept as a principled reference; the real io_uring TCP win lives on a real NIC and in the many-source
/// scaling case (async shared-ring design), not single-stream loopback throughput.
class IoUringTCPMultishotSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "IoUringTCPMultishot";

    explicit IoUringTCPMultishotSource(const SourceDescriptor& sourceDescriptor);
    ~IoUringTCPMultishotSource() override;

    IoUringTCPMultishotSource(const IoUringTCPMultishotSource&) = delete;
    IoUringTCPMultishotSource& operator=(const IoUringTCPMultishotSource&) = delete;
    IoUringTCPMultishotSource(IoUringTCPMultishotSource&&) = delete;
    IoUringTCPMultishotSource& operator=(IoUringTCPMultishotSource&&) = delete;

    /// Not used: preFillsBuffers() is true, so the runner drains takePreFilledBuffer() instead.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// Connect the socket, init the ring + provided buffer ring, arm the first multishot recv.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    [[nodiscard]] bool preFillsBuffers() const override { return true; }

    /// Return the next recv'd buffer in byte order; nullopt at end-of-stream / on stop.
    [[nodiscard]] std::optional<TupleBuffer> takePreFilledBuffer(const std::stop_token& stopToken) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Resolve + connect a blocking TCP socket to host:port. Throws CannotOpenSource on failure.
    void connectSocket();
    /// (Re-)arm the multishot recv into the provided buffer group. Sets multishotArmed.
    void armMultishotRecv();
    /// Replenish ring slot `bid` with a fresh pool buffer (blocking on the pool = backpressure).
    void replenishBuffer(std::uint16_t bid);

    std::string socketHost;
    std::string socketPort;
    int sockfd = -1;

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::size_t bufferSize = 0;

    io_uring ring{};
    bool ringInitialized = false;
    bool socketRegistered = false; ///< sockfd registered as fixed file index 0 (skips per-op fd lookup)

    io_uring_buf_ring* bufRing = nullptr; ///< provided buffer ring (kernel-selected buffers for multishot recv)
    unsigned numBuffers = 0; ///< provided-ring entries (power of two); also the cap on buffers held by the ring
    std::vector<TupleBuffer> slotBuffers; ///< slotBuffers[bid] = the pool buffer currently backing ring slot bid

    std::deque<TupleBuffer> ready; ///< recv'd buffers reaped but not yet handed to the runner (byte order)
    bool multishotArmed = false; ///< a multishot recv is currently armed on the ring
    bool streamClosed = false; ///< peer closed -- drain `ready`, then signal EoS
};

struct ConfigParametersIoUringTCPMultishot
{
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "socketHost",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socketPort",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            const auto portNumber = DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("IoUringTCPMultishot specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, HOST, PORT);
};

}
