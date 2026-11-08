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

#include <PythonProcessSocketSource.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/uio.h>

#include <DataTypes/UnboundField.hpp>
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
#include <PythonProcesses.hpp>
#include <PythonProcessSource.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

namespace
{
/// How long the source waits for its processes and the channel before it checks for a stop request.
constexpr int POLL_INTERVAL_MS = 100;

/// Batches a process has been sent and not answered yet: the one it works on, and the ones that wait in its socket, so that it
/// rarely runs dry and has to be woken. More than 4 did not help.
constexpr size_t MAX_IN_FLIGHT = 4;

/// The pieces of a batch per sendmsg.
constexpr size_t MAX_IOVECS = 64;

constexpr std::array<std::byte, 8> PADDING{};
}

PythonProcessSocketSource::PythonProcessSocketSource(
    const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor, const UdfProcessProtocol protocol)
    : channel(config.channel, PythonProcessChannel::Role::SOURCE)
    , calls(deserializeUdfProcessCalls(config.calls))
    , protocol(protocol)
    , schema(sourceDescriptor.getSchema())
{
}

/// A failed `open` may leave processes behind. `close` does nothing if it already ran.
PythonProcessSocketSource::~PythonProcessSocketSource()
{
    close();
}

std::ostream& PythonProcessSocketSource::toString(std::ostream& str) const
{
    return str << "PythonProcessSocketSource(calls: " << calls.size() << ", processes: " << processes.size() << ")";
}

void PythonProcessSocketSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    layout = std::dynamic_pointer_cast<ArrowBufferRef>(
        LowerSchemaProvider::lowerSchemaToArrow(bufferProvider->getBufferSize(), Schema<QualifiedUnboundField, Ordered>{schema}));
    INVARIANT(layout != nullptr, "lowerSchemaToArrow must return an ArrowBufferRef");
    arrowSchema = makeArrowSchema(*layout);

    /// All processes start before any is waited for, so that they import pyarrow and the UDFs in parallel.
    const auto program = generatePythonBatchWrapper(*layout, calls, protocol);
    for (size_t index = 0; index < PythonProcesses::numberOfProcesses(); ++index)
    {
        auto& process = *processes.emplace_back(std::make_unique<Process>());
        const auto [pid, socket] = PythonProcesses::start(program);
        process.pid = pid;
        process.socket = socket;
    }
    for (auto& process : processes)
    {
        PythonProcesses::awaitStart({.pid = process->pid, .socket = process->socket});
        expect(*process, Step::STATUS, std::as_writable_bytes(std::span{process->words}.first(1)));
        if (protocol == UdfProcessProtocol::RAW_BUFFERS)
        {
            continue;
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
        PythonProcesses::sendAll(
            {.pid = process->pid, .socket = process->socket},
            std::as_bytes(std::span{message->data, static_cast<size_t>(message->size_bytes)}));
    }
    channel->setCapacity(PythonProcesses::BUFFERS_PER_PROCESS * processes.size());
}

void PythonProcessSocketSource::close()
{
    std::vector<PythonProcesses::Process> ended;
    for (const auto& process : processes)
    {
        ended.push_back({.pid = std::exchange(process->pid, -1), .socket = std::exchange(process->socket, -1)});
    }
    PythonProcesses::end(ended);
    processes.clear();
    results.clear();
    spare.reset();
    channel.release();
}

bool PythonProcessSocketSource::feedProcesses()
{
    if (channelEnded)
    {
        return false;
    }
    for (auto& process : processes)
    {
        /// The next batch goes out once the one before it is sent whole, so that it waits in the socket while the process works.
        while (process->inFlight.size() < MAX_IN_FLIGHT && process->sending.empty())
        {
            auto [input, ended] = channel->tryPop();
            if (!input)
            {
                channelEnded = ended;
                return !ended;
            }
            /// An empty buffer still carries its sequence and chunk number and the watermark.
            if (input->getNumberOfTuples() == 0)
            {
                results.push_back({.input = std::move(*input), .output = std::nullopt});
                continue;
            }
            const auto& sent = process->inFlight.emplace_back(std::move(*input));
            if (protocol == UdfProcessProtocol::RAW_BUFFERS)
            {
                prepareRawBatch(*process, sent);
            }
            else
            {
                prepareIpcBatch(*process, sent);
            }
            continueSending(*process);
        }
    }
    return true;
}

void PythonProcessSocketSource::prepareRawBatch(Process& process, const TupleBuffer& input) const
{
    /// The buffers point into `input`, which stays in flight until the answer arrived, so the columns go out without a copy.
    const auto buffers = arrowBuffersOf(input, *layout);

    /// [rows][body size], then per field its buffers as [size][bytes, padded], see UdfProcessProtocol::RAW_BUFFERS.
    process.sizes.assign(2 + buffers.size(), 0);
    for (size_t index = 0; index < buffers.size(); ++index)
    {
        process.sizes[2 + index] = buffers[index].size();
    }
    process.sizes[0] = input.getNumberOfTuples();
    process.sizes[1] = rawBodySize(buffers);
    const auto sizes = std::as_bytes(std::span{process.sizes});
    process.sending.clear();
    process.sending.push_back(sizes.first(2 * sizeof(uint64_t)));
    for (size_t index = 0; index < buffers.size(); ++index)
    {
        process.sending.push_back(sizes.subspan((2 + index) * sizeof(uint64_t), sizeof(uint64_t)));
        if (const auto size = buffers[index].size(); size > 0)
        {
            process.sending.push_back(buffers[index]);
            if (const auto padding = paddedSize(size) - size; padding > 0)
            {
                process.sending.push_back(std::span{PADDING}.first(padding));
            }
        }
    }
}

void PythonProcessSocketSource::prepareIpcBatch(Process& process, const TupleBuffer& input) const
{
    /// The array points into `input`, so only the encoder copies the batch, into the body of the message.
    const auto array = exportArrowArray(input, *layout, *arrowSchema.get());
    ArrowError error{};
    nanoarrow::UniqueArrayView view;
    process.ipcHeader = nanoarrow::UniqueBuffer{};
    process.ipcBody = nanoarrow::UniqueBuffer{};
    if (ArrowArrayViewInitFromSchema(view.get(), arrowSchema.get(), &error) != NANOARROW_OK
        || ArrowArrayViewSetArray(view.get(), array.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderEncodeSimpleRecordBatch(process.encoder.get(), view.get(), process.ipcBody.get(), &error) != NANOARROW_OK
        || ArrowIpcEncoderFinalizeBuffer(process.encoder.get(), 1, process.ipcHeader.get()) != NANOARROW_OK)
    {
        throw UdfExecutionError("Failed to encode a batch for the UDF process: {}", error.message);
    }
    process.sending.clear();
    process.sending.push_back(std::as_bytes(std::span{process.ipcHeader->data, static_cast<size_t>(process.ipcHeader->size_bytes)}));
    if (process.ipcBody->size_bytes > 0)
    {
        process.sending.push_back(std::as_bytes(std::span{process.ipcBody->data, static_cast<size_t>(process.ipcBody->size_bytes)}));
    }
}

void PythonProcessSocketSource::continueSending(Process& process)
{
    while (!process.sending.empty())
    {
        std::array<iovec, MAX_IOVECS> vectors{};
        const auto count = std::min(process.sending.size(), vectors.size());
        for (size_t index = 0; index < count; ++index)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast) sendmsg only reads them
            vectors[index] = {.iov_base = const_cast<std::byte*>(process.sending[index].data()), .iov_len = process.sending[index].size()};
        }
        msghdr message{};
        message.msg_iov = vectors.data();
        message.msg_iovlen = count;
        const auto sent = sendmsg(process.socket, &message, MSG_DONTWAIT | MSG_NOSIGNAL);
        if (sent < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            if (errno == EINTR)
            {
                continue;
            }
            throw UdfExecutionError("The UDF process exited unexpectedly: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        auto remaining = static_cast<size_t>(sent);
        size_t done = 0;
        while (done < process.sending.size() && remaining >= process.sending[done].size())
        {
            remaining -= process.sending[done].size();
            ++done;
        }
        process.sending.erase(process.sending.begin(), process.sending.begin() + static_cast<std::ptrdiff_t>(done));
        if (remaining > 0)
        {
            process.sending.front() = process.sending.front().subspan(remaining);
        }
    }
}

bool PythonProcessSocketSource::continueReceiving(Process& process, const std::stop_token& stopToken)
{
    bool drained = false;
    while (true)
    {
        /// What already arrived serves the steps first, as many as it completes.
        while (true)
        {
            if (const auto available = process.inboxEnd - process.inboxBegin; available > 0 && !process.expected.empty())
            {
                const auto taken = std::min(available, process.expected.size());
                std::memcpy(process.expected.data(), process.inbox.data() + process.inboxBegin, taken);
                process.inboxBegin += taken;
                process.expected = process.expected.subspan(taken);
            }
            if (!process.expected.empty())
            {
                break;
            }
            if (!finishStep(process, stopToken))
            {
                return false;
            }
        }
        /// The last call got less than it asked for, so the socket is empty for now; poll says when more arrives.
        if (drained)
        {
            return true;
        }

        /// A body larger than the inbox is read straight into its output buffer, everything else through the inbox, so that one
        /// call takes all of an answer that has arrived.
        const bool direct = process.expected.size() >= process.inbox.size();
        const auto target = direct ? process.expected : std::span{process.inbox};
        const auto received = recv(process.socket, target.data(), target.size(), MSG_DONTWAIT);
        if (received == 0)
        {
            throw UdfExecutionError("The UDF process exited unexpectedly");
        }
        if (received < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return true;
            }
            if (errno == EINTR)
            {
                continue;
            }
            throw UdfExecutionError("Failed to read from the UDF process: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        const auto size = static_cast<size_t>(received);
        if (direct)
        {
            process.expected = process.expected.subspan(size);
        }
        else
        {
            process.inboxBegin = 0;
            process.inboxEnd = size;
        }
        drained = size < target.size();
    }
}

void PythonProcessSocketSource::expect(Process& process, const Step step, const std::span<std::byte> bytes)
{
    process.step = step;
    process.expected = bytes;
}

bool PythonProcessSocketSource::finishStep(Process& process, const std::stop_token& stopToken)
{
    auto& [status, second] = process.words;
    const auto firstWord = std::as_writable_bytes(std::span{process.words}.first(1));
    switch (process.step)
    {
        case Step::STATUS:
            if (status != 1)
            {
                expect(process, Step::FAILURE_LENGTH, firstWord);
            }
            else if (protocol == UdfProcessProtocol::RAW_BUFFERS)
            {
                expect(process, Step::RAW_HEADER, std::as_writable_bytes(std::span{process.words}));
            }
            else
            {
                expect(process, Step::IPC_PREFIX, firstWord);
            }
            return true;
        case Step::FAILURE_LENGTH:
            process.message.resize(status);
            expect(process, Step::FAILURE_MESSAGE, std::span{process.message});
            return true;
        case Step::FAILURE_MESSAGE:
            throw UdfExecutionError(
                "Python UDF failed: {}", std::string_view{reinterpret_cast<const char*>(process.message.data()), process.message.size()});
        case Step::RAW_HEADER: {
            const auto rows = status;
            if (process.inFlight.empty() || rows != process.inFlight.front().getNumberOfTuples())
            {
                throw UdfExecutionError(
                    "The UDF process returned {} rows for a batch of {}",
                    rows,
                    process.inFlight.empty() ? 0 : process.inFlight.front().getNumberOfTuples());
            }
            return expectBody(process, second, stopToken);
        }
        case Step::IPC_PREFIX: {
            /// An encapsulated IPC message starts with 0xFFFFFFFF and the size of the metadata that follows.
            std::array<int32_t, 2> prefix{};
            std::memcpy(prefix.data(), &status, sizeof(prefix));
            if (prefix[0] != -1 || prefix[1] <= 0)
            {
                throw UdfExecutionError("The UDF process sent no Arrow IPC message, but continuation {} and size {}", prefix[0], prefix[1]);
            }
            process.message.resize(sizeof(prefix) + static_cast<size_t>(prefix[1]));
            std::memcpy(process.message.data(), prefix.data(), sizeof(prefix));
            expect(process, Step::IPC_METADATA, std::span{process.message}.subspan(sizeof(prefix)));
            return true;
        }
        case Step::IPC_METADATA: {
            auto* const decoder = process.decoder.get();
            ArrowError error{};
            const ArrowBufferView metadata{.data = {.data = process.message.data()}, .size_bytes = static_cast<int64_t>(process.message.size())};
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
                expect(process, Step::IPC_PREFIX, firstWord);
                return true;
            }
            if (decoder->message_type != NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH)
            {
                throw UdfExecutionError(
                    "The UDF process sent an Arrow IPC message of type {}, not a record batch", fmt::underlying(decoder->message_type));
            }
            if (process.inFlight.empty())
            {
                throw UdfExecutionError("The UDF process returned a batch it was not sent");
            }
            return expectBody(process, static_cast<uint64_t>(decoder->body_size_bytes), stopToken);
        }
        case Step::BODY:
            if (protocol == UdfProcessProtocol::RAW_BUFFERS)
            {
                finishRawBatch(process);
            }
            else
            {
                finishIpcBatch(process);
            }
            results.push_back({.input = std::move(process.inFlight.front()), .output = std::exchange(process.output, std::nullopt)});
            process.inFlight.pop_front();
            expect(process, Step::STATUS, firstWord);
            return true;
    }
    std::unreachable();
}

bool PythonProcessSocketSource::expectBody(Process& process, const uint64_t bodySize, const std::stop_token& stopToken)
{
    if (spare)
    {
        process.output = std::exchange(spare, std::nullopt);
    }
    while (!process.output)
    {
        if (stopToken.stop_requested())
        {
            return false;
        }
        process.output = bufferProvider->getBufferWithTimeout(PythonProcesses::BUFFER_WAIT);
    }
    process.bodySize = bodySize;
    process.bodyLocation = PythonProcesses::placeBody(*process.output, bodySize, *bufferProvider, *layout);
    /// The body is read straight from the socket into the output buffer, and its header points at the columns, so it is not copied again.
    auto* const body = const_cast<std::byte*>( /// NOLINT(cppcoreguidelines-pro-type-const-cast)
        ArrowBufferRef::resolve(*process.output, process.bodyLocation));
    expect(process, Step::BODY, std::span{body, bodySize});
    return true;
}

void PythonProcessSocketSource::setDecoderSchema(Process& process, const ArrowSchema& received) const
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

void PythonProcessSocketSource::finishIpcBatch(Process& process) const
{
    const auto rows = process.inFlight.front().getNumberOfTuples();
    const auto* const body = ArrowBufferRef::resolve(*process.output, process.bodyLocation);
    ArrowError error{};
    ArrowArrayView* batch = nullptr;
    if (ArrowIpcDecoderDecodeArrayView(
            process.decoder.get(), {.data = {.data = body}, .size_bytes = static_cast<int64_t>(process.bodySize)}, -1, &batch, &error)
            != NANOARROW_OK
        || ArrowArrayViewValidate(batch, NANOARROW_VALIDATION_LEVEL_FULL, &error) != NANOARROW_OK)
    {
        throw UdfExecutionError("The UDF process returned an invalid batch: {}", error.message);
    }
    if (std::cmp_not_equal(batch->length, rows))
    {
        throw UdfExecutionError("The UDF process returned {} rows for a batch of {}", batch->length, rows);
    }
    importArrowArrayView(*batch, body, process.bodyLocation, *process.output, *layout);
}

void PythonProcessSocketSource::finishRawBatch(Process& process) const
{
    importRawBody(
        ArrowBufferRef::resolve(*process.output, process.bodyLocation),
        process.bodySize,
        process.inFlight.front().getNumberOfTuples(),
        process.bodyLocation,
        *process.output,
        *layout,
        *arrowSchema.get());
}

Source::FillTupleBufferResult PythonProcessSocketSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    /// The source loop got this buffer from the pool for the source to fill; the next answer is read into it.
    if (!spare)
    {
        spare = tupleBuffer;
    }
    std::vector<pollfd> descriptors;
    descriptors.reserve(processes.size() + 1);
    while (true)
    {
        const bool open = feedProcesses();
        if (!results.empty())
        {
            break;
        }
        const bool busy = std::ranges::any_of(processes, [](const auto& process) { return !process->inFlight.empty(); });
        if (stopToken.stop_requested() || (!open && !busy))
        {
            return FillTupleBufferResult::eos();
        }

        descriptors.clear();
        for (const auto& process : processes)
        {
            descriptors.push_back(
                {.fd = process->socket, .events = static_cast<int16_t>(POLLIN | (process->sending.empty() ? 0 : POLLOUT)), .revents = 0});
        }
        if (open)
        {
            descriptors.push_back({.fd = channel->notifier(), .events = POLLIN, .revents = 0});
        }
        if (poll(descriptors.data(), descriptors.size(), POLL_INTERVAL_MS) < 0 && errno != EINTR)
        {
            throw UdfExecutionError("Failed to wait for the UDF processes: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        if (open && descriptors.back().revents != 0)
        {
            uint64_t count = 0;
            [[maybe_unused]] const auto read = ::read(channel->notifier(), &count, sizeof(count));
        }
        for (size_t index = 0; index < processes.size(); ++index)
        {
            const auto events = descriptors[index].revents;
            auto& process = *processes[index];
            if ((events & POLLOUT) != 0)
            {
                continueSending(process);
            }
            if ((events & (POLLIN | POLLHUP | POLLERR)) != 0 && !continueReceiving(process, stopToken))
            {
                return FillTupleBufferResult::eos();
            }
        }
    }

    auto [input, output] = std::move(results.front());
    results.pop_front();
    if (output)
    {
        tupleBuffer = std::move(*output);
    }
    else
    {
        /// The buffer goes out as it is, so no answer may be read into it, but one may already be on its way into it.
        const auto isOurs = [&](const std::optional<TupleBuffer>& buffer)
        { return buffer && buffer->getAvailableMemoryArea().data() == tupleBuffer.getAvailableMemoryArea().data(); };
        if (isOurs(spare))
        {
            spare.reset();
        }
        else if (std::ranges::any_of(processes, [&](const auto& process) { return isOurs(process->output); }))
        {
            tupleBuffer = bufferProvider->getBufferBlocking();
        }
    }
    const auto rows = input.getNumberOfTuples();
    tupleBuffer.setOriginId(input.getOriginId());
    tupleBuffer.setSequenceNumber(input.getSequenceNumber());
    tupleBuffer.setChunkNumber(input.getChunkNumber());
    tupleBuffer.setLastChunk(input.isLastChunk());
    tupleBuffer.setWatermark(input.getWatermark());
    tupleBuffer.setCreationTimestampInMS(input.getCreationTimestampInMS());
    return FillTupleBufferResult::withBytes(rows);
}

}
