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

#include <liburing.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/// IoUringTCPSource (registered as "IoUringTCPBlocking") ingests a TCP stream with io_uring recv on its OWN
/// dedicated thread (a BlockingSource) -- the variant meant to OUTPERFORM the async (asio epoll+recv) TCPSource.
///
/// Why blocking, not async: the async io_uring TCP source has to bridge ring completions back into asio via an
/// eventfd that asio waits on with epoll -- so it pays epoll's cost (the very thing it tried to avoid) PLUS the
/// io_uring submit, and ends up SLOWER than plain asio on a bandwidth-bound stream. A dedicated thread waits on
/// the ring DIRECTLY (io_uring_wait_cqe_timeout), so there is no epoll hop: one io_uring_enter per recv batch,
/// recv landing straight into the pool buffer (zero copy beyond the kernel's), registered socket fd to skip the
/// per-op fd lookup. The timeout on the wait keeps it responsive to a query stop.
///
/// Like the io_uring file source it uses the PREFILL hook: it fills each pool buffer COMPLETELY (re-issuing recv
/// into the same buffer at the running offset until full) before emitting -- emitting one engine buffer per recv
/// chunk would inflate the in-flight buffer count past the worker pool and deadlock the sink. Buffers are emitted
/// in byte order (single sequential stream), so the CSV formatter can stitch tuples across them.
class IoUringTCPSource final : public BlockingSource
{
public:
    static constexpr std::string_view NAME = "IoUringTCPBlocking";

    explicit IoUringTCPSource(const SourceDescriptor& sourceDescriptor);
    ~IoUringTCPSource() override;

    IoUringTCPSource(const IoUringTCPSource&) = delete;
    IoUringTCPSource& operator=(const IoUringTCPSource&) = delete;
    IoUringTCPSource(IoUringTCPSource&&) = delete;
    IoUringTCPSource& operator=(IoUringTCPSource&&) = delete;

    /// Not used: preFillsBuffers() is true, so the runner drains takePreFilledBuffer() instead.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken, size_t offset) override;

    /// Connect the socket and init the io_uring ring.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    [[nodiscard]] bool preFillsBuffers() const override { return true; }

    /// Fill the current buffer with recv()s until full, then return it; nullopt at end-of-stream / on stop.
    [[nodiscard]] std::optional<TupleBuffer> takePreFilledBuffer(const std::stop_token& stopToken) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// Resolve + connect a blocking TCP socket to host:port. Throws CannotOpenSource on failure.
    void connectSocket();
    /// Submit one recv into fillBuffer at [fillOffset, bufferSize). Sets recvInFlight.
    void submitRecv();

    std::string socketHost;
    std::string socketPort;
    int sockfd = -1;

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::size_t bufferSize = 0;

    io_uring ring{};
    bool ringInitialized = false;
    bool socketRegistered = false; ///< sockfd registered as fixed file index 0 (skips per-op fd lookup)

    TupleBuffer fillBuffer; ///< the pool buffer currently being filled by recv()s (emitted once full)
    std::size_t fillOffset = 0; ///< bytes already recv'd into fillBuffer
    std::uint64_t fillStartMicros = 0; ///< when fillBuffer started filling (stamped as sourceCreationTimestamp)
    bool recvInFlight = false; ///< a recv SQE is submitted and awaiting completion
    bool streamClosed = false; ///< peer closed -- emit the partial tail then signal EoS
};

struct ConfigParametersIoUringTCPBlocking
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
                NES_ERROR(
                    "IoUringTCPBlocking specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, HOST, PORT);
};

}
