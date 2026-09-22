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

#include <TCPSource.hpp>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <future>
#include <memory>
#include <stop_token>
#include <string_view>
#include <fcntl.h>
#include <unistd.h>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Regression tests for issue #79: a mid-stream socket error must not be reported as end-of-stream.
class TCPSourceReadClassificationTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TCPSourceReadClassificationTest.log", LogLevel::LOG_DEBUG); }
};

/// A clean end-of-stream (read() returns 0) is the only case that may terminate the stream.
TEST_F(TCPSourceReadClassificationTest, ZeroBytesIsEndOfStream)
{
    EXPECT_EQ(TCPSource::classifyReadResult(0, 0), TCPSource::ReadOutcome::EndOfStream);
    /// errno is irrelevant for a 0-byte return.
    EXPECT_EQ(TCPSource::classifyReadResult(0, ECONNRESET), TCPSource::ReadOutcome::EndOfStream);
}

/// The core of #79: a genuine mid-stream error (read() returns -1) must NOT be treated as end-of-stream.
TEST_F(TCPSourceReadClassificationTest, ConnectionResetIsErrorNotEndOfStream)
{
    const auto outcome = TCPSource::classifyReadResult(-1, ECONNRESET);
    EXPECT_EQ(outcome, TCPSource::ReadOutcome::Error);
    EXPECT_NE(outcome, TCPSource::ReadOutcome::EndOfStream);
}

/// Other hard errno values are also surfaced as errors rather than swallowed.
TEST_F(TCPSourceReadClassificationTest, OtherHardErrorsAreErrors)
{
    EXPECT_EQ(TCPSource::classifyReadResult(-1, ECONNREFUSED), TCPSource::ReadOutcome::Error);
    EXPECT_EQ(TCPSource::classifyReadResult(-1, EBADF), TCPSource::ReadOutcome::Error);
    EXPECT_EQ(TCPSource::classifyReadResult(-1, EIO), TCPSource::ReadOutcome::Error);
}

/// A receive-timeout / non-blocking no-data condition must be a poll-again, not an error or end-of-stream.
TEST_F(TCPSourceReadClassificationTest, WouldBlockIsPollAgain)
{
    EXPECT_EQ(TCPSource::classifyReadResult(-1, EAGAIN), TCPSource::ReadOutcome::WouldBlock);
    EXPECT_EQ(TCPSource::classifyReadResult(-1, EWOULDBLOCK), TCPSource::ReadOutcome::WouldBlock);
}

/// An interrupted read must be retried, not aborted.
TEST_F(TCPSourceReadClassificationTest, InterruptedReadIsRetry)
{
    EXPECT_EQ(TCPSource::classifyReadResult(-1, EINTR), TCPSource::ReadOutcome::Retry);
}

/// A positive return means data was received.
TEST_F(TCPSourceReadClassificationTest, PositiveBytesAreData)
{
    EXPECT_EQ(TCPSource::classifyReadResult(1, 0), TCPSource::ReadOutcome::Data);
    EXPECT_EQ(TCPSource::classifyReadResult(4096, 0), TCPSource::ReadOutcome::Data);
}

/// Regression tests for issue #131: a peer that sends fewer bytes than one TupleBuffer and then closes
/// the connection must make the source flush the partial buffer and report end-of-stream on the next
/// call. Before the fix the drain loop spun on a sticky read()==0 (100% CPU, query never completed)
/// under the default flush_interval_ms=0.
class TCPSourcePartialThenCloseTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TCPSourcePartialThenCloseTest.log", LogLevel::LOG_DEBUG); }

protected:
    /// Closes both socketpair ends on every exit path (including an early ASSERT failure) so a failing
    /// test cannot leak file descriptors.
    struct SocketPairGuard
    {
        std::array<int, 2> fds{-1, -1};

        ~SocketPairGuard()
        {
            for (const int fd : fds)
            {
                if (fd >= 0)
                {
                    ::close(fd);
                }
            }
        }
    };

    /// Runs one fillTupleBuffer on a worker thread and fails (rather than hanging the suite) if it does
    /// not return within the timeout, which is exactly the #131 spin. request_stop() only breaks the
    /// drain between reads, so it relies on the injected fd carrying a receive timeout (set below) to
    /// bound any read() the worker might be parked in; together they guarantee the worker unwinds.
    /// Each call gets a fresh stop_source so a prior timeout can never pre-stop a later call.
    static Source::FillTupleBufferResult drainWithWatchdog(TCPSource& source, TupleBuffer& tupleBuffer, const char* what)
    {
        std::stop_source stopSource;
        auto pending = std::async(std::launch::async, [&] { return source.fillTupleBuffer(tupleBuffer, stopSource.get_token()); });
        if (pending.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        {
            stopSource.request_stop();
            ADD_FAILURE() << "fillTupleBuffer hung " << what << " (#131 regression)";
        }
        return pending.get();
    }
};

TEST_F(TCPSourcePartialThenCloseTest, PartialBufferThenPeerCloseFlushesThenEoSWithoutHanging)
{
    /// A socketpair stands in for the TCP connection: one end is handed to the source, the other writes
    /// a partial buffer and closes, reproducing "peer sends < one TupleBuffer, then FIN".
    SocketPairGuard sockets;
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.fds.data()), 0) << "socketpair failed: " << std::strerror(errno);

    /// Give the source-side fd a receive timeout so a hypothetical future regression cannot leave the
    /// worker blocked forever inside read() (which request_stop cannot interrupt), keeping the watchdog
    /// effective. The bytes are already buffered and the peer closes below, so read() never blocks here.
    const timeval receiveTimeout{.tv_sec = 2, .tv_usec = 0};
    ASSERT_EQ(::setsockopt(sockets.fds[0], SOL_SOCKET, SO_RCVTIMEO, &receiveTimeout, sizeof(receiveTimeout)), 0)
        << "setsockopt(SO_RCVTIMEO) failed: " << std::strerror(errno);

    constexpr std::string_view payload = "partial-record";
    ASSERT_EQ(::write(sockets.fds[1], payload.data(), payload.size()), static_cast<ssize_t>(payload.size()));
    /// Peer closes after a partial write, so read() on fds[0] yields the bytes once and then 0 forever.
    ASSERT_EQ(::close(sockets.fds[1]), 0);
    sockets.fds[1] = -1; /// closed above; prevent the guard from double-closing it

    /// flush_interval_ms = 0 is the default and the exact trigger for #131. The source now owns fds[0];
    /// the guard still closes it at scope exit (the source's destructor does not).
    TCPSource source{TCPSource::InjectedSocketTag{}, sockets.fds[0], "127.0.0.1", "0", 0.0F};

    constexpr size_t totalMemory = 10UL * 1024 * 8192;
    auto bufferManager = BufferManager::create(totalMemory, 0.9, BufferAlignment{64}, 8192, std::make_shared<NesDefaultMemoryAllocator>());
    auto tupleBuffer = bufferManager->getBufferBlocking();

    /// First call flushes the partial bytes and must return promptly (a hang here is the #131 spin).
    const auto firstResult = drainWithWatchdog(source, tupleBuffer, "on the partial buffer");
    ASSERT_FALSE(firstResult.isEoS());
    EXPECT_EQ(firstResult.getNumberOfBytes(), payload.size());

    /// Second call must report end-of-stream, also promptly.
    const auto secondResult = drainWithWatchdog(source, tupleBuffer, "after the partial buffer was drained");
    EXPECT_TRUE(secondResult.isEoS());
}

/// Regression tests for issues #145/#146: an idle-but-open connection (peer connected, sending nothing)
/// must keep polling without pinning a CPU core, and must unwind promptly when the query is stopped. The
/// bounded, stop-token-interruptible backoff in the WouldBlock path is what guarantees the latter.
class TCPSourceIdlePollTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TCPSourceIdlePollTest.log", LogLevel::LOG_DEBUG); }

protected:
    struct SocketPairGuard
    {
        std::array<int, 2> fds{-1, -1};

        ~SocketPairGuard()
        {
            for (const int fd : fds)
            {
                if (fd >= 0)
                {
                    ::close(fd);
                }
            }
        }
    };
};

TEST_F(TCPSourceIdlePollTest, IdleOpenConnectionKeepsPollingAndStopsPromptly)
{
    /// A socketpair whose peer stays connected but never sends data reproduces an idle source. The
    /// source-side fd is made non-blocking so read() returns EAGAIN immediately, which is both the
    /// #146 leftover-non-blocking case and the worst case for the #145 busy-spin.
    SocketPairGuard sockets;
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.fds.data()), 0) << "socketpair failed: " << std::strerror(errno);

    const int existingFlags = ::fcntl(sockets.fds[0], F_GETFL, 0);
    ASSERT_NE(existingFlags, -1) << "fcntl(F_GETFL) failed: " << std::strerror(errno);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg, hicpp-signed-bitwise, concurrency-mt-unsafe): POSIX varargs, single-threaded.
    ASSERT_EQ(::fcntl(sockets.fds[0], F_SETFL, existingFlags | O_NONBLOCK), 0) << "fcntl(F_SETFL) failed: " << std::strerror(errno);

    TCPSource source{TCPSource::InjectedSocketTag{}, sockets.fds[0], "127.0.0.1", "0", 0.0F};

    constexpr double unpooledMemoryFraction = 0.9;
    constexpr uint32_t bufferAlignmentBytes = 64; ///< a cache line
    constexpr uint32_t bufferSizeBytes = 8192;
    constexpr size_t totalMemory = 10UL * 1024 * bufferSizeBytes;
    auto bufferManager = BufferManager::create(
        totalMemory,
        unpooledMemoryFraction,
        BufferAlignment{bufferAlignmentBytes},
        bufferSizeBytes,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto tupleBuffer = bufferManager->getBufferBlocking();

    std::stop_source stopSource;
    auto pending = std::async(std::launch::async, [&] { return source.fillTupleBuffer(tupleBuffer, stopSource.get_token()); });

    /// While the connection is open and quiet the source must keep polling rather than report a spurious
    /// end-of-stream, so the drain must still be running here.
    EXPECT_EQ(pending.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout)
        << "idle-open connection wrongly terminated the drain";

    /// A stop request must interrupt the backoff wait and let the drain unwind promptly.
    stopSource.request_stop();
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(5)), std::future_status::ready)
        << "idle drain did not stop promptly after request_stop (busy-spin / non-interruptible backoff)";
    EXPECT_TRUE(pending.get().isEoS());
}

/// Regression test for #130 (harden #99): the classifyReadResult unit tests above only exercise the pure
/// classifier; they never confirm that fillBuffer's read loop actually wires ReadOutcome::Error to a thrown
/// RunningRoutineFailure rather than the old "treat as EoS" behavior that silently truncated results. This
/// drives a real mid-stream socket error end to end through fillTupleBuffer.
class TCPSourceHardErrorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TCPSourceHardErrorTest.log", LogLevel::LOG_DEBUG); }

protected:
    struct SocketGuard
    {
        int fd = -1;

        ~SocketGuard()
        {
            if (fd >= 0)
            {
                ::close(fd);
            }
        }
    };
};

TEST_F(TCPSourceHardErrorTest, MidStreamConnectionResetThrowsRunningRoutineFailureNotEoS)
{
    /// SO_LINGER{onoff=1, linger=0}'s abortive close only produces ECONNRESET on a real TCP connection (it
    /// is a no-op on an AF_UNIX socketpair, which just observes a clean EoS), so this uses an actual loopback
    /// TCP connection: a listener accepts one connection, and the accepted (server) side is the one aborted.
    SocketGuard listener;
    listener.fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener.fd, 0) << "socket() failed: " << std::strerror(errno);

    sockaddr_in listenAddr{};
    listenAddr.sin_family = AF_INET;
    listenAddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    listenAddr.sin_port = 0; /// let the kernel pick a free port
    ASSERT_EQ(::bind(listener.fd, reinterpret_cast<sockaddr*>(&listenAddr), sizeof(listenAddr)), 0)
        << "bind() failed: " << std::strerror(errno);
    ASSERT_EQ(::listen(listener.fd, 1), 0) << "listen() failed: " << std::strerror(errno);

    sockaddr_in boundAddr{};
    socklen_t boundAddrLen = sizeof(boundAddr);
    ASSERT_EQ(::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&boundAddr), &boundAddrLen), 0)
        << "getsockname() failed: " << std::strerror(errno);

    SocketGuard client;
    client.fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client.fd, 0) << "socket() failed: " << std::strerror(errno);
    ASSERT_EQ(::connect(client.fd, reinterpret_cast<sockaddr*>(&boundAddr), sizeof(boundAddr)), 0)
        << "connect() failed: " << std::strerror(errno);

    SocketGuard accepted;
    accepted.fd = ::accept(listener.fd, nullptr, nullptr);
    ASSERT_GE(accepted.fd, 0) << "accept() failed: " << std::strerror(errno);

    /// Aborting the accepted (server) side with SO_LINGER{onoff=1, linger=0} makes its close() send a TCP
    /// RST instead of a FIN, so the client-side read() below observes ECONNRESET rather than a clean EoS.
    const linger abortiveClose{.l_onoff = 1, .l_linger = 0};
    ASSERT_EQ(::setsockopt(accepted.fd, SOL_SOCKET, SO_LINGER, &abortiveClose, sizeof(abortiveClose)), 0)
        << "setsockopt(SO_LINGER) failed: " << std::strerror(errno);
    ASSERT_EQ(::close(accepted.fd), 0);
    accepted.fd = -1; /// closed above; prevent the guard from double-closing it

    /// The source does not take ownership of the fd for closing purposes (its destructor does not close it),
    /// so the guard still closes client.fd at scope exit.
    TCPSource source{TCPSource::InjectedSocketTag{}, client.fd, "127.0.0.1", "0", 0.0F};

    constexpr double unpooledMemoryFraction = 0.9;
    constexpr uint32_t bufferAlignmentBytes = 64; ///< a cache line
    constexpr uint32_t bufferSizeBytes = 8192;
    constexpr size_t totalMemory = 10UL * 1024 * bufferSizeBytes;
    auto bufferManager = BufferManager::create(
        totalMemory,
        unpooledMemoryFraction,
        BufferAlignment{bufferAlignmentBytes},
        bufferSizeBytes,
        std::make_shared<NesDefaultMemoryAllocator>());
    auto tupleBuffer = bufferManager->getBufferBlocking();

    std::stop_source stopSource;
    try
    {
        [[maybe_unused]] const auto result = source.fillTupleBuffer(tupleBuffer, stopSource.get_token());
        FAIL() << "Expected fillTupleBuffer to throw RunningRoutineFailure on a mid-stream socket error";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::RunningRoutineFailure)
            << "A hard socket error must be surfaced as RunningRoutineFailure, not silently treated as end-of-stream";
    }
}

}
