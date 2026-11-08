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

#include <PythonProcessSource.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <signal.h> /// NOLINT(modernize-deprecated-headers) kill
#include <spawn.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>
#include <nanoarrow/nanoarrow_ipc.h>
#include <nanoarrow/nanoarrow_ipc.hpp>
#include <ArrowBatches.hpp>
#include <ErrorHandling.hpp>
#include <PythonBatchWrapper.hpp>
#include <PythonProcessChannel.hpp>
#include <UdfProcessCall.hpp>

extern char** environ; /// NOLINT(readability-redundant-declaration) not declared by every libc

namespace NES
{

namespace
{
/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> CHANNEL{
    Identifier::parse("CHANNEL"),
    "Id of the channel the PythonProcess sink of the same UDF operator hands its buffers to",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};

const ConfigField<std::string> CALLS{
    Identifier::parse("CALLS"),
    "The serialized calls of the UDF operator",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

/// The file descriptor of the socket in the process.
constexpr int PROCESS_SOCKET_FD = 3;
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
    /// Set by the worker from worker.python_udf.venv
    if (const auto* const venv = std::getenv("NES_UDF_VENV"); venv != nullptr && *venv != '\0') /// NOLINT(concurrency-mt-unsafe)
    {
        return (std::filesystem::path{venv} / "bin" / "python").string();
    }
    return "python3";
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

/// The buffers the channel holds per process: more than a process takes in the retry interval of the sink, at about a millisecond
/// per batch.
constexpr size_t BUFFERS_PER_PROCESS = 16;

/// How long a thread that serves a process waits for a buffer before it checks for a stop request.
constexpr auto BUFFER_WAIT = std::chrono::milliseconds(25);

std::span<const std::byte> bytesOf(const uint64_t& value)
{
    return std::as_bytes(std::span{&value, 1});
}
}

Schema<QualifiedErasedConfigField, Ordered> PythonProcessSource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("PYTHON_PROCESS_SOURCE"), CHANNEL, CALLS);
}

std::expected<PythonProcessSourceConfig, Exception> PythonProcessSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return PythonProcessSourceConfig{.channel = config.get(CHANNEL), .calls = config.get(CALLS)};
}

PythonProcessSource::PythonProcessSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : channel(config.channel, PythonProcessChannel::Role::SOURCE)
    , calls(deserializeUdfProcessCalls(config.calls))
    , schema(sourceDescriptor.getSchema())
{
}

/// A failed `open` may leave processes behind. `close` does nothing if it already ran.
PythonProcessSource::~PythonProcessSource()
{
    close();
}

std::ostream& PythonProcessSource::toString(std::ostream& str) const
{
    return str << "PythonProcessSource(calls: " << calls.size() << ", processes: " << processes.size() << ")";
}

void PythonProcessSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    layout = std::dynamic_pointer_cast<ArrowBufferRef>(
        LowerSchemaProvider::lowerSchemaToArrow(bufferProvider->getBufferSize(), Schema<QualifiedUnboundField, Ordered>{schema}));
    INVARIANT(layout != nullptr, "lowerSchemaToArrow must return an ArrowBufferRef");
    arrowSchema = makeArrowSchema(*layout);

    /// All processes start before any is waited for, so that they import pyarrow and the UDFs in parallel.
    const auto program = generatePythonBatchWrapper(*layout, calls);
    const std::stop_token noStop;
    const uint64_t programSize = program.size();
    for (size_t index = 0; index < numberOfProcesses(); ++index)
    {
        auto& process = *processes.emplace_back(std::make_unique<Process>());
        startProcess(process);
        sendAll(process, bytesOf(programSize), noStop);
        sendAll(process, std::as_bytes(std::span{program}), noStop);
    }
    for (auto& process : processes)
    {
        try
        {
            receiveStatus(*process, noStop);
        }
        catch (const Exception& exception)
        {
            throw CannotLoadUdf("Failed to initialize the UDF process: {}", exception.what());
        }

        /// The IPC stream to the process starts with the schema of the batches.
        ArrowError error{};
        nanoarrow::UniqueBuffer message;
        if (ArrowIpcEncoderInit(process->encoder.get()) != NANOARROW_OK || ArrowIpcDecoderInit(process->decoder.get()) != NANOARROW_OK
            || ArrowIpcEncoderEncodeSchema(process->encoder.get(), arrowSchema.get(), &error) != NANOARROW_OK
            || ArrowIpcEncoderFinalizeBuffer(process->encoder.get(), 1, message.get()) != NANOARROW_OK)
        {
            throw CannotLoadUdf("Failed to encode the Arrow schema of a UDF process: {}", error.message);
        }
        sendAll(*process, std::as_bytes(std::span{message->data, static_cast<size_t>(message->size_bytes)}), noStop);
    }

    channel->setCapacity(BUFFERS_PER_PROCESS * processes.size());
    serving = processes.size();
    for (auto& process : processes)
    {
        servers.emplace_back([this, &process = *process](const std::stop_token& stopToken) { serve(process, stopToken); });
    }
}

void PythonProcessSource::startProcess(Process& process) const
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
    /// Nothing else of the worker reaches the process, such as its network sockets.
    posix_spawn_file_actions_addclosefrom_np(&actions, PROCESS_SOCKET_FD + 1);

    auto executable = pythonExecutable();
    std::string flag = "-c";
    std::string bootstrap = BOOTSTRAP;
    std::array<char*, 4> arguments{executable.data(), flag.data(), bootstrap.data(), nullptr};
    const auto spawned = posix_spawnp(&process.pid, executable.c_str(), &actions, nullptr, arguments.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    ::close(sockets[1]);
    if (spawned != 0)
    {
        ::close(sockets[0]);
        process.pid = -1;
        throw CannotLoadUdf(
            "Failed to start the UDF process '{}': {}", executable, std::strerror(spawned)); /// NOLINT(concurrency-mt-unsafe)
    }
    process.socket = sockets[0];
}

void PythonProcessSource::close()
{
    /// The threads stop before the sockets they use close.
    for (auto& server : servers)
    {
        server.request_stop();
    }
    servers.clear();
    /// Without its socket, a process ends once it finished the batch it is working on.
    for (const auto& process : processes)
    {
        if (process->socket >= 0)
        {
            ::close(process->socket);
            process->socket = -1;
        }
    }
    const auto deadline = std::chrono::steady_clock::now() + TERMINATION_GRACE_PERIOD;
    for (const auto& process : processes)
    {
        while (process->pid > 0 && waitpid(process->pid, nullptr, WNOHANG) == 0)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                kill(process->pid, SIGKILL);
                waitpid(process->pid, nullptr, 0);
                break;
            }
            std::this_thread::sleep_for(TERMINATION_POLL_INTERVAL);
        }
        process->pid = -1;
    }
    processes.clear();
    {
        const std::scoped_lock lock(resultsMutex);
        results.clear();
    }
    channel.release();
}

bool PythonProcessSource::sendAll(const Process& process, std::span<const std::byte> bytes, const std::stop_token& stopToken)
{
    while (!bytes.empty())
    {
        if (stopToken.stop_requested())
        {
            return false;
        }
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
    return true;
}

bool PythonProcessSource::receiveAll(const Process& process, std::span<std::byte> bytes, const std::stop_token& stopToken)
{
    while (!bytes.empty())
    {
        if (stopToken.stop_requested())
        {
            return false;
        }
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
    return true;
}

bool PythonProcessSource::receiveStatus(const Process& process, const std::stop_token& stopToken)
{
    uint64_t status = 0;
    if (!receiveAll(process, std::as_writable_bytes(std::span{&status, 1}), stopToken))
    {
        return false;
    }
    if (status == 1)
    {
        return true;
    }
    uint64_t length = 0;
    if (!receiveAll(process, std::as_writable_bytes(std::span{&length, 1}), stopToken))
    {
        return false;
    }
    std::string message(length, '\0');
    if (!receiveAll(process, std::as_writable_bytes(std::span{message}), stopToken))
    {
        return false;
    }
    throw UdfExecutionError("Python UDF failed: {}", message);
}

bool PythonProcessSource::sendBatch(Process& process, const TupleBuffer& input, const std::stop_token& stopToken) const
{
    /// The array points into `input`, so only the encoder copies the batch, into the body of the message.
    const auto array = exportArrowArray(input, *layout, *arrowSchema.get());
    ArrowError error{};
    nanoarrow::UniqueArrayView view;
    nanoarrow::UniqueBuffer body;
    nanoarrow::UniqueBuffer header;
    if (ArrowArrayViewInitFromSchema(view.get(), arrowSchema.get(), &error) != NANOARROW_OK
        || ArrowArrayViewSetArray(view.get(), array.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderEncodeSimpleRecordBatch(process.encoder.get(), view.get(), body.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderFinalizeBuffer(process.encoder.get(), 1, header.get()) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to encode a batch for the UDF process: {}", error.message);
    }
    return sendAll(process, std::as_bytes(std::span{header->data, static_cast<size_t>(header->size_bytes)}), stopToken)
        && sendAll(process, std::as_bytes(std::span{body->data, static_cast<size_t>(body->size_bytes)}), stopToken);
}

bool PythonProcessSource::receiveMessage(const Process& process, std::vector<std::byte>& message, const std::stop_token& stopToken)
{
    /// An encapsulated IPC message starts with 0xFFFFFFFF and the size of the metadata that follows.
    std::array<int32_t, 2> prefix{};
    if (!receiveAll(process, std::as_writable_bytes(std::span{prefix}), stopToken))
    {
        return false;
    }
    if (prefix[0] != -1 || prefix[1] <= 0)
    {
        throw UdfExecutionError("The UDF process sent no Arrow IPC message, but continuation {} and size {}", prefix[0], prefix[1]);
    }
    message.resize(sizeof(prefix) + static_cast<size_t>(prefix[1]));
    std::memcpy(message.data(), prefix.data(), sizeof(prefix));
    return receiveAll(process, std::span{message}.subspan(sizeof(prefix)), stopToken);
}

void PythonProcessSource::setDecoderSchema(Process& process, const ArrowSchema& received) const
{
    /// The process answers with the schema it received, so anything else is a bug in the wrapper or the UDF.
    bool matches = received.n_children == arrowSchema->n_children;
    for (int64_t index = 0; matches && index < received.n_children; ++index)
    {
        matches = std::string_view{received.children[index]->format} == std::string_view{arrowSchema->children[index]->format};
    }
    if (!matches)
    {
        throw UdfExecutionError("The UDF process answered with a schema other than the one of its batches");
    }
    ArrowError error{};
    nanoarrow::UniqueSchema expected;
    if (ArrowSchemaDeepCopy(arrowSchema.get(), expected.get()) != NANOARROW_OK
        || ArrowIpcDecoderSetEndianness(process.decoder.get(), process.decoder->endianness) != NANOARROW_OK
        || ArrowIpcDecoderSetSchema(process.decoder.get(), expected.get(), &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to read the schema of the UDF process: {}", error.message);
    }
}

bool PythonProcessSource::receiveBatch(Process& process, TupleBuffer& output, const uint64_t rows, const std::stop_token& stopToken) const
{
    if (!receiveStatus(process, stopToken))
    {
        return false;
    }
    auto* const decoder = process.decoder.get();
    ArrowError error{};
    std::vector<std::byte> message;
    while (true)
    {
        if (!receiveMessage(process, message, stopToken))
        {
            return false;
        }
        const ArrowBufferView metadata{.data = {.data = message.data()}, .size_bytes = static_cast<int64_t>(message.size())};
        if (ArrowIpcDecoderVerifyHeader(decoder, metadata, &error) != NANOARROW_OK
            || ArrowIpcDecoderDecodeHeader(decoder, metadata, &error) != NANOARROW_OK)
        {
            throw UdfExecutionError("Failed to read a message of the UDF process: {}", error.message);
        }
        if (decoder->message_type == NANOARROW_IPC_MESSAGE_TYPE_SCHEMA)
        {
            nanoarrow::UniqueSchema received;
            if (ArrowIpcDecoderDecodeSchema(decoder, received.get(), &error) != NANOARROW_OK)
            {
                throw UdfExecutionError("Failed to read the schema of the UDF process: {}", error.message);
            }
            setDecoderSchema(process, *received.get());
            continue;
        }
        if (decoder->message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH)
        {
            throw UdfExecutionError(
                "The UDF process sent an Arrow IPC message of type {}, not a record batch", fmt::underlying(decoder->message_type));
        }
        break;
    }

    /// The body goes behind the header of `output` if it fits, and into a child buffer otherwise. Either way it is read straight from
    /// the socket and the header points at its columns, so the batch is not copied again.
    const auto bodySize = static_cast<uint64_t>(decoder->body_size_bytes);
    const auto headerSize = layout->getHeaderSize();
    ArrowBufferRef::Location bodyLocation{.buffer = ArrowBufferRef::Location::MAIN_BUFFER, .offset = static_cast<uint32_t>(headerSize)};
    if (headerSize + bodySize > output.getBufferSize())
    {
        auto child = [&]
        {
            if (bodySize <= bufferProvider->getBufferSize())
            {
                return bufferProvider->getBufferBlocking();
            }
            auto unpooled = bufferProvider->getUnpooledBuffer(bodySize);
            if (!unpooled.has_value())
            {
                throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", bodySize);
            }
            return *unpooled;
        }();
        child.setNumberOfTuples(bodySize);
        bodyLocation = {.buffer = output.storeChildBuffer(child).getRawValue() + 1, .offset = 0};
    }
    auto* const body
        = const_cast<std::byte*>(ArrowBufferRef::resolve(output, bodyLocation)); /// NOLINT(cppcoreguidelines-pro-type-const-cast)
    if (!receiveAll(process, std::span{body, bodySize}, stopToken))
    {
        return false;
    }

    ArrowArrayView* batch = nullptr;
    if (ArrowIpcDecoderDecodeArrayView(decoder, {.data = {.data = body}, .size_bytes = static_cast<int64_t>(bodySize)}, -1, &batch, &error)
            != NANOARROW_OK
        || ArrowArrayViewValidate(batch, NANOARROW_VALIDATION_LEVEL_FULL, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("The UDF process returned an invalid batch: {}", error.message);
    }
    if (std::cmp_not_equal(batch->length, rows))
    {
        throw UdfExecutionError("The UDF process returned {} rows for a batch of {}", batch->length, rows);
    }
    importArrowArrayView(*batch, body, bodyLocation, output, *layout);
    return true;
}

bool PythonProcessSource::pushResult(Result result, const std::stop_token& stopToken)
{
    {
        std::unique_lock lock(resultsMutex);
        /// Bounds the finished batches that wait for the source thread, as they hold buffers.
        if (!resultsChanged.wait(lock, stopToken, [this] { return results.size() < 2 * processes.size(); }))
        {
            return false;
        }
        results.push_back(std::move(result));
    }
    resultsChanged.notify_all();
    return true;
}

void PythonProcessSource::serve(Process& process, const std::stop_token& stopToken)
{
    try
    {
        while (auto input = channel->pop(stopToken))
        {
            Result result{.input = *input, .output = std::nullopt, .failure = nullptr};
            /// An empty buffer still carries its sequence and chunk number and the watermark.
            if (const auto rows = input->getNumberOfTuples(); rows > 0)
            {
                std::optional<TupleBuffer> output;
                while (!output && !stopToken.stop_requested())
                {
                    output = bufferProvider->getBufferWithTimeout(BUFFER_WAIT);
                }
                if (!output || !sendBatch(process, *input, stopToken) || !receiveBatch(process, *output, rows, stopToken))
                {
                    break;
                }
                result.output = std::move(output);
            }
            if (!pushResult(std::move(result), stopToken))
            {
                break;
            }
        }
    }
    catch (...)
    {
        pushResult(Result{.input = {}, .output = std::nullopt, .failure = std::current_exception()}, stopToken);
    }
    {
        const std::scoped_lock lock(resultsMutex);
        --serving;
    }
    resultsChanged.notify_all();
}

Source::FillTupleBufferResult PythonProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    Result result;
    {
        std::unique_lock lock(resultsMutex);
        if (!resultsChanged.wait(lock, stopToken, [this] { return !results.empty() || serving == 0; }) || results.empty())
        {
            return FillTupleBufferResult::eos();
        }
        result = std::move(results.front());
        results.pop_front();
    }
    resultsChanged.notify_all();
    if (result.failure)
    {
        std::rethrow_exception(result.failure);
    }
    const auto& input = result.input;
    const auto rows = input.getNumberOfTuples();
    /// The thread that served the batch wrote it to a buffer of its own, which takes the place of the one the source thread got.
    if (result.output)
    {
        tupleBuffer = std::move(*result.output);
    }
    tupleBuffer.setOriginId(input.getOriginId());
    tupleBuffer.setSequenceNumber(input.getSequenceNumber());
    tupleBuffer.setChunkNumber(input.getChunkNumber());
    tupleBuffer.setLastChunk(input.isLastChunk());
    tupleBuffer.setWatermark(input.getWatermark());
    tupleBuffer.setCreationTimestampInMS(input.getCreationTimestampInMS());
    return FillTupleBufferResult::withBytes(rows);
}

}
