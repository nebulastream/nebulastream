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
#include <atomic>
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
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <sched.h>
#include <signal.h> /// NOLINT(modernize-deprecated-headers) kill
#include <spawn.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CpuPinning.hpp>
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

/// The program of a host process, which serves several UDF operators: over its socket it receives, per operator, a socket, the shared
/// memory with the rings of the operator and the pool of the worker as file descriptors, and the program generatePythonBatchWrapper
/// made for the operator. It runs each program in a module of its own and then handles the requests of all operators in turn, one
/// at a time each. An operator leaves when the worker closes its socket; the process ends when the worker closes the socket of the
/// process. With NES_UDF_SHARED_PROCESSES=threads, the one host process runs each operator in a thread of its own instead, which
/// runs at the same time as the others only on a Python without a GIL, such as a free-threaded CPython (python3.14t).
constexpr const char* HOST = R"(import mmap, os, select, socket, struct, threading, time, traceback, types

_THREADED = os.environ.get("NES_UDF_SHARED_PROCESSES") == "threads"

_SPIN_SECONDS = 1e-4
_SLEEP_SECONDS = 1e-3
_operators = []
_pools = {}


def _cpu_list():
    # The CPUs of worker.python_udf.process_cpus, e.g. 64-79,96; each operator thread pins itself to the next one, as a process does
    cpus = []
    for part in os.environ.get("NES_UDF_PROCESS_CPUS", "").split(","):
        if part:
            first, _, last = part.partition("-")
            cpus.extend(range(int(first), int(last or first) + 1))
    return cpus


_CPUS = _cpu_list()
_threads_started = 0


def _run(operator, cpu):
    if cpu is not None:
        os.sched_setaffinity(0, {cpu})
    operator._main()


def _read(size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise SystemExit(1)
        data += chunk
    return bytes(data)


def _pool(fd):
    # All operators share the one mapping of the pool of the worker, each through a memoryview of its own: the threads of
    # NES_UDF_SHARED_PROCESSES=threads slice it at the same time, which one shared memoryview does not survive without a GIL
    status = os.fstat(fd)
    key = (status.st_dev, status.st_ino)
    if key not in _pools:
        _pools[key] = mmap.mmap(fd, 0)
    os.close(fd)
    return memoryview(_pools[key])


def _attached(operator):
    pass


def _attach():
    # [program size] with the file descriptors, then the program; False once the worker closed the socket
    sock.setblocking(True)
    try:
        data, fds, _, _ = socket.recv_fds(sock, 8, 3)
        if not data:
            return False
        data += _read(8 - len(data))
        program = _read(struct.unpack("<Q", data)[0])
    finally:
        sock.setblocking(False)
    channel = socket.socket(fileno=fds[0])
    operator = types.ModuleType("nes_udf_operator")
    operator.sock = channel
    operator._HOSTED = True
    operator._SHARED_FD = fds[1]
    operator._POOL = _pool(fds[2]) if len(fds) > 2 else None
    try:
        exec(compile(program, "<nes-udf-operator>", "exec"), operator.__dict__)
    except SystemExit:
        # The program told the worker why it failed
        channel.close()
        return True
    except BaseException as error:
        message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
        channel.sendall(struct.pack("<QQ", 0, len(message)) + message)
        channel.close()
        return True
    finally:
        os.close(fds[1])
    channel.setblocking(False)
    _attached(operator)
    if _THREADED:
        # The thread waits for requests and handles them as a process of its own would, see SHARED_MEMORY_START
        global _threads_started
        cpu = _CPUS[_threads_started % len(_CPUS)] if _CPUS else None
        _threads_started += 1
        threading.Thread(target=_run, args=(operator, cpu), daemon=True).start()
        return True
    _operators.append(operator)
    return True


def _pending():
    for operator in _operators:
        if operator._control[operator._REQUEST_TAIL] != operator._head:
            return True
    return False


def _events(timeout):
    # New operators, wake-ups and operators that left; False once the worker closed the socket
    readable, _, _ = select.select([sock] + [operator.sock for operator in _operators], [], [], timeout)
    for ready in readable:
        if ready is sock:
            if not _attach():
                return False
            continue
        try:
            left = not ready.recv(4096)
        except BlockingIOError:
            left = False
        except ConnectionError:
            # The operator closed its socket with wake-ups unread; the other operators continue
            left = True
        if left:
            _operators[:] = [operator for operator in _operators if operator.sock is not ready]
            ready.close()
    return True


def _idle():
    # Spins briefly, as a wake-up costs both sides a system call, then sleeps until the worker wakes an operator or sends a new one
    deadline = time.perf_counter() + _SPIN_SECONDS
    while time.perf_counter() < deadline:
        if _pending():
            return True
    for operator in _operators:
        operator._control[operator._PROCESS_SLEEPS] = 1
    try:
        if _pending():
            return True
        return _events(_SLEEP_SECONDS)
    finally:
        for operator in _operators:
            operator._control[operator._PROCESS_SLEEPS] = 0


def _serve():
    if _THREADED:
        while _attach():
            pass
        return
    sock.setblocking(False)
    rounds = 0
    while True:
        worked = False
        for operator in _operators:
            if operator._step():
                worked = True
        rounds += 1
        if not worked:
            if not _idle():
                return
        elif not rounds & 255:
            if not _events(0):
                return


_serve()
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

/// Pins a process to the next CPU of NES_UDF_PROCESS_CPUS, which the worker sets from worker.python_udf.process_cpus. Without it, the
/// process gets the CPUs of the worker back, as it starts with the ones of the source thread that started it.
/// The host process of NES_UDF_SHARED_PROCESSES=threads may run on all of these CPUs, as each of its operator threads pins itself to
/// one of them, see HOST.
void pin(const pid_t pid)
{
    static std::atomic<size_t> started{0};
    const auto* const list = std::getenv("NES_UDF_PROCESS_CPUS"); /// NOLINT(concurrency-mt-unsafe)
    const auto cpus = CpuPinning::parseCpuList(list == nullptr ? "" : list).value_or(std::vector<int>{});
    if (!cpus.empty() && threadedProcess())
    {
        cpu_set_t set;
        CPU_ZERO(&set);
        for (const auto cpu : cpus)
        {
            CPU_SET(cpu, &set);
        }
        if (sched_setaffinity(pid, sizeof(set), &set) == 0)
        {
            return;
        }
    }
    if (cpus.empty() || !CpuPinning::pinProcess(pid, cpus[started.fetch_add(1) % cpus.size()]))
    {
        CpuPinning::unpinProcess(pid);
    }
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

namespace
{
Process spawn(std::string_view program, int sharedFd, int poolFd);

/// The host processes of the worker and how many UDF operators use them.
struct Hosts
{
    std::mutex mutex;
    std::vector<Process> processes;
    size_t attached = 0;
};

Hosts& hosts()
{
    static Hosts instance;
    return instance;
}

/// Makes the host process `index` serve a UDF operator, see HOST; starts the host processes up to it if they do not run yet.
Process attach(const size_t index, const std::string_view program, const int sharedFd, const int poolFd)
{
    auto& [mutex, processes, attached] = hosts();
    const std::scoped_lock lock{mutex};
    while (processes.size() <= index)
    {
        processes.push_back(spawn(HOST, -1, -1));
    }
    const auto& host = processes[index];

    std::array<int, 2> sockets{};
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets.data()) != 0)
    {
        throw CannotLoadUdf("Failed to create the socket of a UDF operator: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
    }
    std::array<int, 3> descriptors{sockets[1], sharedFd, poolFd};
    const size_t count = poolFd >= 0 ? 3 : 2;
    uint64_t size = program.size();
    iovec bytes{.iov_base = &size, .iov_len = sizeof(size)};
    alignas(cmsghdr) std::array<char, CMSG_SPACE(3 * sizeof(int))> control{};
    msghdr message{};
    message.msg_iov = &bytes;
    message.msg_iovlen = 1;
    message.msg_control = control.data();
    message.msg_controllen = CMSG_SPACE(count * sizeof(int));
    auto* const header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(count * sizeof(int));
    std::memcpy(CMSG_DATA(header), descriptors.data(), count * sizeof(int));
    ssize_t sent = 0;
    do
    {
        sent = sendmsg(host.socket, &message, MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    ::close(sockets[1]);
    if (sent != static_cast<ssize_t>(sizeof(size)))
    {
        ::close(sockets[0]);
        throw CannotLoadUdf("Failed to reach a UDF host process: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
    }
    try
    {
        sendAll(host, std::as_bytes(std::span{program}));
    }
    catch (...)
    {
        ::close(sockets[0]);
        throw;
    }
    ++attached;
    return Process{.pid = -1, .socket = sockets[0], .hosted = true};
}
}

bool sharedProcesses()
{
    const auto* const shared = std::getenv("NES_UDF_SHARED_PROCESSES"); /// NOLINT(concurrency-mt-unsafe)
    return shared != nullptr && (std::string_view{shared} == "1" || std::string_view{shared} == "threads");
}

bool threadedProcess()
{
    const auto* const shared = std::getenv("NES_UDF_SHARED_PROCESSES"); /// NOLINT(concurrency-mt-unsafe)
    return shared != nullptr && std::string_view{shared} == "threads";
}

Process start(const std::string_view program, const int sharedFd, const int poolFd, const size_t index)
{
    if (sharedFd >= 0 && sharedProcesses())
    {
        return attach(threadedProcess() ? 0 : index, program, sharedFd, poolFd);
    }
    return spawn(program, sharedFd, poolFd);
}

namespace
{
Process spawn(const std::string_view program, const int sharedFd, const int poolFd)
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
        &actions,
        (poolFd >= 0         ? PROCESS_POOL_FD
             : sharedFd >= 0 ? PROCESS_SHARED_FD
                             : PROCESS_SOCKET_FD)
            + 1);

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
    pin(process.pid);

    const uint64_t size = program.size();
    sendAll(process, std::as_bytes(std::span{&size, 1}));
    sendAll(process, std::as_bytes(std::span{program}));
    return process;
}
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
    size_t left = 0;
    for (auto& process : processes)
    {
        if (process.socket >= 0)
        {
            ::close(process.socket);
            process.socket = -1;
        }
        if (std::exchange(process.hosted, false))
        {
            ++left;
        }
    }
    if (left > 0)
    {
        /// The host processes end with the last UDF operator they serve.
        std::vector<Process> ended;
        {
            auto& [mutex, hostProcesses, attached] = hosts();
            const std::scoped_lock lock{mutex};
            attached -= left;
            if (attached == 0)
            {
                ended = std::exchange(hostProcesses, {});
            }
        }
        PythonProcesses::end(ended);
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
