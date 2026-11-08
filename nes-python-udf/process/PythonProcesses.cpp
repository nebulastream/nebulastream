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

#include <PythonProcesses.hpp>

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include <poll.h>
#include <signal.h> /// NOLINT(modernize-deprecated-headers) kill
#include <spawn.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PythonBatchWrapper.hpp>

extern char** environ; /// NOLINT(readability-redundant-declaration) not declared by every libc

namespace NES::PythonProcesses
{

namespace
{
/// The file descriptors of the socket and of the shared memory in the process.
constexpr int PROCESS_SOCKET_FD = 3;
constexpr int PROCESS_SHARED_FD = 4;
constexpr int PROCESS_POOL_FD = 5;
constexpr int POLL_INTERVAL_MS = 100;
constexpr auto TERMINATION_GRACE_PERIOD = std::chrono::seconds(2);
constexpr auto TERMINATION_POLL_INTERVAL = std::chrono::milliseconds(10);

/// Receives the program over the socket, so that its size is not limited by the command line.
constexpr const char* BOOTSTRAP = R"(import socket, struct
sock = socket.socket(fileno=3)
def read(size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise SystemExit(1)
        data += chunk
    return bytes(data)
exec(compile(read(struct.unpack("<Q", read(8))[0]), "<nes-udf-process>", "exec"), {"sock": sock})
)";

std::string pythonExecutable()
{
    /// Set by the worker from worker.python_udf.process_venv and worker.python_udf.venv
    for (const auto* const variable : {"NES_UDF_PROCESS_VENV", "NES_UDF_VENV"})
    {
        if (const auto* const venv = std::getenv(variable); venv != nullptr && *venv != '\0') /// NOLINT(concurrency-mt-unsafe)
        {
            return (std::filesystem::path{venv} / "bin" / "python").string();
        }
    }
    return "python3";
}
}

UdfProcessProtocol protocol()
{
    /// Set by the worker from worker.python_udf.process_protocol
    const auto* const name = std::getenv("NES_UDF_PROCESS_PROTOCOL"); /// NOLINT(concurrency-mt-unsafe)
    const std::string_view protocol = name == nullptr ? "" : name;
    if (protocol == "RAW_BUFFERS")
    {
        return UdfProcessProtocol::RAW_BUFFERS;
    }
    if (protocol == "SHARED_MEMORY")
    {
        return UdfProcessProtocol::SHARED_MEMORY;
    }
    if (protocol == "SHARED_MEMORY_ZERO_COPY")
    {
        return UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY;
    }
    return UdfProcessProtocol::ARROW_IPC;
}

size_t numberOfProcesses()
{
    /// Set by the worker from worker.python_udf.processes, or its number of worker threads
    if (const auto* const processes = std::getenv("NES_UDF_PROCESSES"); processes != nullptr) /// NOLINT(concurrency-mt-unsafe)
    {
        size_t number = 0;
        if (const auto [end, error] = std::from_chars(processes, processes + std::strlen(processes), number);
            error == std::errc{} && *end == '\0' && number > 0)
        {
            return number;
        }
    }
    return 1;
}

Process start(const std::string_view program, const int sharedFd, const int poolFd)
{
    std::array<int, 2> sockets{};
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()) != 0)
    {
        throw CannotLoadUdf("Failed to create the socket of a UDF process: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
    }

    posix_spawn_file_actions_t actions{};
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null", 0, 0);
    posix_spawn_file_actions_adddup2(&actions, sockets[1], PROCESS_SOCKET_FD);
    if (sharedFd >= 0)
    {
        posix_spawn_file_actions_adddup2(&actions, sharedFd, PROCESS_SHARED_FD);
    }
    if (poolFd >= 0)
    {
        posix_spawn_file_actions_adddup2(&actions, poolFd, PROCESS_POOL_FD);
    }
    /// Nothing else of the worker reaches the process, such as its network sockets.
    posix_spawn_file_actions_addclosefrom_np(
        &actions, (poolFd >= 0 ? PROCESS_POOL_FD : sharedFd >= 0 ? PROCESS_SHARED_FD : PROCESS_SOCKET_FD) + 1);

    auto executable = pythonExecutable();
    std::string flag = "-c";
    std::string bootstrap = BOOTSTRAP;
    std::array<char*, 4> arguments{executable.data(), flag.data(), bootstrap.data(), nullptr};
    Process process;
    const auto spawned = posix_spawnp(&process.pid, executable.c_str(), &actions, nullptr, arguments.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    ::close(sockets[1]);
    if (spawned != 0)
    {
        ::close(sockets[0]);
        throw CannotLoadUdf(
            "Failed to start the UDF process '{}': {}", executable, std::strerror(spawned)); /// NOLINT(concurrency-mt-unsafe)
    }
    process.socket = sockets[0];

    const uint64_t size = program.size();
    sendAll(process, std::as_bytes(std::span{&size, 1}));
    sendAll(process, std::as_bytes(std::span{program}));
    return process;
}

void awaitStart(const Process& process)
{
    try
    {
        uint64_t status = 0;
        receiveAll(process, std::as_writable_bytes(std::span{&status, 1}));
        if (status == 1)
        {
            return;
        }
        uint64_t length = 0;
        receiveAll(process, std::as_writable_bytes(std::span{&length, 1}));
        std::string message(length, '\0');
        receiveAll(process, std::as_writable_bytes(std::span{message}));
        throw UdfExecutionError("Python UDF failed: {}", message);
    }
    catch (const Exception& exception)
    {
        throw CannotLoadUdf("Failed to initialize the UDF process: {}", exception.what());
    }
}

void sendAll(const Process& process, std::span<const std::byte> bytes)
{
    while (!bytes.empty())
    {
        pollfd descriptor{.fd = process.socket, .events = POLLOUT, .revents = 0};
        if (poll(&descriptor, 1, POLL_INTERVAL_MS) <= 0)
        {
            continue;
        }
        const auto sent = send(process.socket, bytes.data(), bytes.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
        if (sent < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            {
                continue;
            }
            throw UdfExecutionError("The UDF process exited unexpectedly: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        bytes = bytes.subspan(static_cast<size_t>(sent));
    }
}

void receiveAll(const Process& process, std::span<std::byte> bytes)
{
    while (!bytes.empty())
    {
        pollfd descriptor{.fd = process.socket, .events = POLLIN, .revents = 0};
        if (poll(&descriptor, 1, POLL_INTERVAL_MS) <= 0)
        {
            continue;
        }
        const auto received = recv(process.socket, bytes.data(), bytes.size(), MSG_DONTWAIT);
        if (received == 0)
        {
            throw UdfExecutionError("The UDF process exited unexpectedly");
        }
        if (received < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
            {
                continue;
            }
            throw UdfExecutionError("Failed to read from the UDF process: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        bytes = bytes.subspan(static_cast<size_t>(received));
    }
}

void end(const std::span<Process> processes)
{
    for (auto& process : processes)
    {
        if (process.socket >= 0)
        {
            ::close(process.socket);
            process.socket = -1;
        }
    }
    const auto deadline = std::chrono::steady_clock::now() + TERMINATION_GRACE_PERIOD;
    for (auto& process : processes)
    {
        while (process.pid > 0 && waitpid(process.pid, nullptr, WNOHANG) == 0)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                kill(process.pid, SIGKILL);
                waitpid(process.pid, nullptr, 0);
                break;
            }
            std::this_thread::sleep_for(TERMINATION_POLL_INTERVAL);
        }
        process.pid = -1;
    }
}

ArrowBufferRef::Location
placeBody(TupleBuffer& output, const uint64_t bodySize, AbstractBufferProvider& bufferProvider, const ArrowBufferRef& layout)
{
    const auto headerSize = layout.getHeaderSize();
    if (headerSize + bodySize <= output.getBufferSize())
    {
        return {.buffer = ArrowBufferRef::Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(headerSize)};
    }
    auto child = [&]
    {
        if (bodySize <= bufferProvider.getBufferSize())
        {
            return bufferProvider.getBufferBlocking();
        }
        auto unpooled = bufferProvider.getUnpooledBuffer(bodySize);
        if (!unpooled.has_value())
        {
            throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", bodySize);
        }
        return *unpooled;
    }();
    child.setNumberOfTuples(bodySize);
    return {.buffer = output.storeChildBuffer(child).getRawValue() + 1, .offset = 0};
}

}
