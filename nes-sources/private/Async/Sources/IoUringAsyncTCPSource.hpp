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

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <Async/Sources/IoUringAsyncSource.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/// IoUringAsyncTCPSource ingests a TCP stream with io_uring recv, integrated into the async io_context via
/// the base's eventfd completion bridge. It targets the asio (epoll + recv) TCPSource baseline.
///
/// Design -- zero-copy OFFSET-FILL recv (not multishot): each recv lands DIRECTLY in the current pool buffer
/// at the current write offset; on a partial recv we re-issue another recv into the same buffer at the new
/// offset, and only emit the buffer once it is FULL (or at EoS, the partial tail). This is the io_uring
/// analog of asio's async_read (which loops recv until the buffer fills). Emitting only full buffers is
/// essential: a TCP stream delivers small chunks, and emitting one engine buffer per recv chunk would inflate
/// the in-flight buffer count past the worker pool and deadlock the sink (that is exactly what a naive
/// multishot + provided-buffer-ring version did -- partial completions, one full-size pool slot each).
///
/// Pipelining: when a full buffer is emitted, the recv for the NEXT buffer is submitted BEFORE returning, so
/// the kernel fills buffer N+1 while the engine processes buffer N. Ordering is trivially correct (a single
/// sequential recv stream), so the CSV formatter can stitch tuples across consecutive buffers. The win over
/// asio is the cheaper submit/complete path (no epoll_wait per readiness) and the overlapped next-recv.
class IoUringAsyncTCPSource final : public IoUringAsyncSource
{
public:
    static constexpr std::string_view NAME = "IoUringTCP";

    explicit IoUringAsyncTCPSource(const SourceDescriptor& sourceDescriptor);

    IoUringAsyncTCPSource(const IoUringAsyncTCPSource&) = delete;
    IoUringAsyncTCPSource& operator=(const IoUringAsyncTCPSource&) = delete;
    IoUringAsyncTCPSource(IoUringAsyncTCPSource&&) = delete;
    IoUringAsyncTCPSource& operator=(IoUringAsyncTCPSource&&) = delete;

    /// Fill the current buffer with recv()s until full, then emit it; nullopt at end-of-stream / cancellation.
    asio::awaitable<std::optional<TupleBuffer>, Executor> takePreFilledBuffer() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

protected:
    asio::awaitable<void, Executor> onOpen() override;
    void onClose() override;

private:
    /// Submit one recv into fillBuffer at [fillOffset, bufferSize). The caller submits the ring afterwards.
    void submitRecv();

    const std::string socketHost;
    const std::string socketPort;

    std::optional<asio::ip::tcp::socket> socket;
    int socketFd = -1;

    TupleBuffer fillBuffer; ///< the pool buffer currently being filled by recv()s (emitted once full)
    std::size_t fillOffset = 0; ///< bytes already recv'd into fillBuffer
    std::uint64_t fillStartMicros = 0; ///< when fillBuffer started filling (stamped as sourceCreationTimestamp)
    bool streamClosed = false; ///< peer closed (recv returned 0) -- emit the partial tail then signal EoS
};

struct ConfigParametersIoUringTCP
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
                NES_ERROR("IoUringTCP specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SourceDescriptor::parameterMap, HOST, PORT);
};

}
