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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <filesystem>
#include <memory>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
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

/// A failed `open` may leave a process behind. `close` does nothing if it already ran.
PythonProcessSource::~PythonProcessSource()
{
    close();
}

std::ostream& PythonProcessSource::toString(std::ostream& str) const
{
    return str << "PythonProcessSource(calls: " << calls.size() << ")";
}

void PythonProcessSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    layout = std::dynamic_pointer_cast<ArrowBufferRef>(
        LowerSchemaProvider::lowerSchemaToArrow(bufferProvider->getBufferSize(), Schema<QualifiedUnboundField, Ordered>{schema}));
    INVARIANT(layout != nullptr, "lowerSchemaToArrow must return an ArrowBufferRef");
    arrowSchema = makeArrowSchema(*layout);
    startProcess();

    const auto program = generatePythonBatchWrapper(*layout, calls);
    const std::stop_token noStop;
    const uint64_t programSize = program.size();
    sendAll(bytesOf(programSize), noStop);
    sendAll(std::as_bytes(std::span{program}), noStop);
    try
    {
        receiveStatus(noStop);
    }
    catch (const Exception& exception)
    {
        throw CannotLoadUdf("Failed to initialize the UDF process: {}", exception.what());
    }

    /// The IPC stream to the process starts with the schema of the batches.
    ArrowError error{};
    nanoarrow::UniqueBuffer message;
    if (ArrowIpcEncoderInit(encoder.get()) != NANOARROW_OK || ArrowIpcDecoderInit(decoder.get()) != NANOARROW_OK
        || ArrowIpcEncoderEncodeSchema(encoder.get(), arrowSchema.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderFinalizeBuffer(encoder.get(), 1, message.get()) != NANOARROW_OK)
    {
        throw CannotLoadUdf("Failed to encode the Arrow schema of a UDF process: {}", error.message);
    }
    sendAll(std::as_bytes(std::span{message->data, static_cast<size_t>(message->size_bytes)}), noStop);
}

void PythonProcessSource::startProcess()
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
    const auto spawned = posix_spawnp(&process, executable.c_str(), &actions, nullptr, arguments.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    ::close(sockets[1]);
    if (spawned != 0)
    {
        ::close(sockets[0]);
        process = -1;
        throw CannotLoadUdf(
            "Failed to start the UDF process '{}': {}", executable, std::strerror(spawned)); /// NOLINT(concurrency-mt-unsafe)
    }
    processSocket = sockets[0];
}

void PythonProcessSource::close()
{
    /// Without its socket, the process ends once it finished the batch it is working on.
    if (processSocket >= 0)
    {
        ::close(processSocket);
        processSocket = -1;
    }
    if (process > 0)
    {
        const auto deadline = std::chrono::steady_clock::now() + TERMINATION_GRACE_PERIOD;
        while (waitpid(process, nullptr, WNOHANG) == 0)
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                kill(process, SIGKILL);
                waitpid(process, nullptr, 0);
                break;
            }
            std::this_thread::sleep_for(TERMINATION_POLL_INTERVAL);
        }
        process = -1;
    }
    channel.release();
}

bool PythonProcessSource::sendAll(std::span<const std::byte> bytes, const std::stop_token& stopToken) const
{
    while (!bytes.empty())
    {
        if (stopToken.stop_requested())
        {
            return false;
        }
        pollfd descriptor{.fd = processSocket, .events = POLLOUT, .revents = 0};
        if (poll(&descriptor, 1, POLL_INTERVAL_MS) <= 0)
        {
            continue;
        }
        const auto sent = send(processSocket, bytes.data(), bytes.size(), MSG_DONTWAIT | MSG_NOSIGNAL);
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

bool PythonProcessSource::receiveAll(std::span<std::byte> bytes, const std::stop_token& stopToken) const
{
    while (!bytes.empty())
    {
        if (stopToken.stop_requested())
        {
            return false;
        }
        pollfd descriptor{.fd = processSocket, .events = POLLIN, .revents = 0};
        if (poll(&descriptor, 1, POLL_INTERVAL_MS) <= 0)
        {
            continue;
        }
        const auto received = recv(processSocket, bytes.data(), bytes.size(), MSG_DONTWAIT);
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

bool PythonProcessSource::receiveStatus(const std::stop_token& stopToken) const
{
    uint64_t status = 0;
    if (!receiveAll(std::as_writable_bytes(std::span{&status, 1}), stopToken))
    {
        return false;
    }
    if (status == 1)
    {
        return true;
    }
    uint64_t length = 0;
    if (!receiveAll(std::as_writable_bytes(std::span{&length, 1}), stopToken))
    {
        return false;
    }
    std::string message(length, '\0');
    if (!receiveAll(std::as_writable_bytes(std::span{message}), stopToken))
    {
        return false;
    }
    throw UdfExecutionError("Python UDF failed: {}", message);
}

bool PythonProcessSource::sendBatch(const TupleBuffer& input, const std::stop_token& stopToken)
{
    /// The array points into `input`, so only the encoder copies the batch, into the body of the message.
    const auto array = exportArrowArray(input, *layout, *arrowSchema.get());
    ArrowError error{};
    nanoarrow::UniqueArrayView view;
    nanoarrow::UniqueBuffer body;
    nanoarrow::UniqueBuffer header;
    if (ArrowArrayViewInitFromSchema(view.get(), arrowSchema.get(), &error) != NANOARROW_OK
        || ArrowArrayViewSetArray(view.get(), array.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderEncodeSimpleRecordBatch(encoder.get(), view.get(), body.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderFinalizeBuffer(encoder.get(), 1, header.get()) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to encode a batch for the UDF process: {}", error.message);
    }
    return sendAll(std::as_bytes(std::span{header->data, static_cast<size_t>(header->size_bytes)}), stopToken)
        && sendAll(std::as_bytes(std::span{body->data, static_cast<size_t>(body->size_bytes)}), stopToken);
}

bool PythonProcessSource::receiveMessage(std::vector<std::byte>& message, const std::stop_token& stopToken)
{
    /// An encapsulated IPC message starts with 0xFFFFFFFF and the size of the metadata that follows.
    std::array<int32_t, 2> prefix{};
    if (!receiveAll(std::as_writable_bytes(std::span{prefix}), stopToken))
    {
        return false;
    }
    if (prefix[0] != -1 || prefix[1] <= 0)
    {
        throw UdfExecutionError("The UDF process sent no Arrow IPC message, but continuation {} and size {}", prefix[0], prefix[1]);
    }
    message.resize(sizeof(prefix) + static_cast<size_t>(prefix[1]));
    std::memcpy(message.data(), prefix.data(), sizeof(prefix));
    return receiveAll(std::span{message}.subspan(sizeof(prefix)), stopToken);
}

void PythonProcessSource::setDecoderSchema(const ArrowSchema& received)
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
        || ArrowIpcDecoderSetEndianness(decoder.get(), decoder->endianness) != NANOARROW_OK
        || ArrowIpcDecoderSetSchema(decoder.get(), expected.get(), &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to read the schema of the UDF process: {}", error.message);
    }
}

bool PythonProcessSource::receiveBatch(TupleBuffer& output, const uint64_t rows, const std::stop_token& stopToken)
{
    if (!receiveStatus(stopToken))
    {
        return false;
    }
    ArrowError error{};
    std::vector<std::byte> message;
    while (true)
    {
        if (!receiveMessage(message, stopToken))
        {
            return false;
        }
        const ArrowBufferView metadata{.data = {.data = message.data()}, .size_bytes = static_cast<int64_t>(message.size())};
        if (ArrowIpcDecoderVerifyHeader(decoder.get(), metadata, &error) != NANOARROW_OK
            || ArrowIpcDecoderDecodeHeader(decoder.get(), metadata, &error) != NANOARROW_OK)
        {
            throw UdfExecutionError("Failed to read a message of the UDF process: {}", error.message);
        }
        if (decoder->message_type == NANOARROW_IPC_MESSAGE_TYPE_SCHEMA)
        {
            nanoarrow::UniqueSchema received;
            if (ArrowIpcDecoderDecodeSchema(decoder.get(), received.get(), &error) != NANOARROW_OK)
            {
                throw UdfExecutionError("Failed to read the schema of the UDF process: {}", error.message);
            }
            setDecoderSchema(*received.get());
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
    if (!receiveAll(std::span{body, bodySize}, stopToken))
    {
        return false;
    }

    ArrowArrayView* batch = nullptr;
    if (ArrowIpcDecoderDecodeArrayView(
            decoder.get(), {.data = {.data = body}, .size_bytes = static_cast<int64_t>(bodySize)}, -1, &batch, &error)
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

Source::FillTupleBufferResult PythonProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    const auto input = channel->pop(stopToken);
    if (!input.has_value())
    {
        return FillTupleBufferResult::eos();
    }
    const auto rows = input->getNumberOfTuples();
    /// An empty buffer still carries its sequence and chunk number and the watermark.
    if (rows > 0 && (!sendBatch(*input, stopToken) || !receiveBatch(tupleBuffer, rows, stopToken)))
    {
        return FillTupleBufferResult::eos();
    }
    tupleBuffer.setOriginId(input->getOriginId());
    tupleBuffer.setSequenceNumber(input->getSequenceNumber());
    tupleBuffer.setChunkNumber(input->getChunkNumber());
    tupleBuffer.setLastChunk(input->isLastChunk());
    tupleBuffer.setWatermark(input->getWatermark());
    tupleBuffer.setCreationTimestampInMS(input->getCreationTimestampInMS());
    return FillTupleBufferResult::withBytes(rows);
}

}
