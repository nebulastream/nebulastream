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

#include <PythonProcessShmSource.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string_view>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/socket.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/SharedMemoryAllocator.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/format.h>
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
/// The layout of the shared memory, see UdfProcessProtocol::SHARED_MEMORY.
constexpr size_t CONTROL_SIZE = 4096;
/// Large enough for many batches of pooled buffers in flight, and for a batch with large VARSIZED values.
constexpr size_t RING_SIZE = size_t{4} * 1024 * 1024;
constexpr size_t MEMORY_SIZE = CONTROL_SIZE + (2 * RING_SIZE);
constexpr uint64_t WRAP = ~uint64_t{0};

/// Words of the control block, each on a cache line of its own.
enum class Word : size_t
{
    REQUEST_TAIL = 0,
    REQUEST_HEAD = 8,
    ANSWER_TAIL = 16,
    ANSWER_HEAD = 24,
    PROCESS_SLEEPS = 32,
    WORKER_SLEEPS = 40,
};

constexpr size_t REQUEST_HEADER_SIZE = 3 * sizeof(uint64_t);
/// SHARED_MEMORY_ZERO_COPY: [record size][rows][children][value area offset][value area size][buffer offset], then [offset][length]
/// per child buffer, see SHARED_MEMORY_IN_PLACE; the answer has [nulls][where][offset][length] per call.
constexpr size_t IN_PLACE_REQUEST_HEADER_SIZE = 6 * sizeof(uint64_t);
constexpr size_t IN_PLACE_REQUEST_ENTRY_SIZE = 2 * sizeof(uint64_t);
constexpr size_t IN_PLACE_ANSWER_ENTRY_SIZE = 4 * sizeof(uint64_t);
constexpr uint64_t INLINE = uint64_t{1} << 63U;
/// Where the VARSIZED values of a result are.
enum class Where : uint64_t
{
    NONE = 0,
    VALUE_AREA = 1,
    INLINE = 2,
};
constexpr size_t ANSWER_HEADER_SIZE = 4 * sizeof(uint64_t);

/// The value areas a process holds at most: the one it writes into and the ones it moves on to when that is full, before the answer
/// that tells the source to give it another arrives.
constexpr size_t MAX_VALUE_AREAS = 3;

/// Batches a process has been sent and not answered yet. Sending one costs no system call, so more of them only cost buffers.
constexpr size_t MAX_IN_FLIGHT = 8;

/// How long the source spins when there is nothing to do before it sleeps, and how long it sleeps at most, which bounds a missed
/// wake-up, see SHARED_MEMORY_RUNTIME.
constexpr auto SPIN_DURATION = std::chrono::microseconds(50);
constexpr int SLEEP_MS = 1;

std::atomic_ref<uint64_t> word(std::byte* memory, Word word)
{
    return std::atomic_ref<uint64_t>{*reinterpret_cast<uint64_t*>(memory + (static_cast<size_t>(word) * sizeof(uint64_t)))}; /// NOLINT
}

std::byte* requestRing(std::byte* memory)
{
    return memory + CONTROL_SIZE;
}

std::byte* answerRing(std::byte* memory)
{
    return memory + CONTROL_SIZE + RING_SIZE;
}

uint64_t load(const std::byte* bytes)
{
    uint64_t value = 0;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

void store(std::byte* bytes, const uint64_t value)
{
    std::memcpy(bytes, &value, sizeof(value));
}

void wake(const PythonProcesses::Process& process)
{
    constexpr std::byte BYTE{1};
    /// A full socket already holds wake-ups.
    [[maybe_unused]] const auto sent = send(process.socket, &BYTE, 1, MSG_DONTWAIT | MSG_NOSIGNAL);
}

void spinPause()
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
}
}

PythonProcessShmSource::PythonProcessShmSource(
    const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor, const UdfProcessProtocol protocol)
    : channel(config.channel, PythonProcessChannel::Role::SOURCE)
    , zeroCopy(protocol == UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY)
    , calls(deserializeUdfProcessCalls(config.calls))
    , schema(sourceDescriptor.getSchema())
{
}

/// A failed `open` may leave processes behind. `close` does nothing if it already ran.
PythonProcessShmSource::~PythonProcessShmSource()
{
    close();
}

std::ostream& PythonProcessShmSource::toString(std::ostream& str) const
{
    return str << "PythonProcessShmSource(calls: " << calls.size() << ", processes: " << processes.size() << ", zero copy: " << zeroCopy
               << ")";
}

void PythonProcessShmSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    layout = std::dynamic_pointer_cast<ArrowBufferRef>(
        LowerSchemaProvider::lowerSchemaToArrow(bufferProvider->getBufferSize(), Schema<QualifiedUnboundField, Ordered>{schema}));
    INVARIANT(layout != nullptr, "lowerSchemaToArrow must return an ArrowBufferRef");
    arrowSchema = makeArrowSchema(*layout);
    varSizedResults = std::ranges::any_of(
        calls, [&](const UdfProcessCall& call) { return layout->getFields().at(call.resultField).type.type == DataType::Type::VARSIZED; });

    if (zeroCopy)
    {
        pool = SharedMemoryAllocator::primary();
        if (!pool)
        {
            throw CannotLoadUdf("SHARED_MEMORY_ZERO_COPY needs a worker whose buffer pool is shared memory");
        }
    }

    /// All processes start before any is waited for, so that they import the UDFs in parallel.
    const auto program = generatePythonBatchWrapper(
        *layout, calls, zeroCopy ? UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY : UdfProcessProtocol::SHARED_MEMORY);
    for (size_t index = 0; index < PythonProcesses::numberOfProcesses(); ++index)
    {
        auto& process = *processes.emplace_back(std::make_unique<Process>());
        const int file = memfd_create("nes-udf-process", MFD_CLOEXEC);
        if (file < 0)
        {
            throw CannotLoadUdf("Failed to create the shared memory of a UDF process: {}", std::strerror(errno)); /// NOLINT
        }
        void* memory = MAP_FAILED;
        if (ftruncate(file, MEMORY_SIZE) == 0)
        {
            memory = mmap(nullptr, MEMORY_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
        }
        if (memory == MAP_FAILED)
        {
            const auto error = errno;
            ::close(file);
            throw CannotLoadUdf("Failed to map the shared memory of a UDF process: {}", std::strerror(error)); /// NOLINT
        }
        process.memory = static_cast<std::byte*>(memory);
        try
        {
            process.handle = PythonProcesses::start(program, file, pool ? pool->fd : -1);
        }
        catch (...)
        {
            ::close(file);
            throw;
        }
        ::close(file);
    }
    for (const auto& process : processes)
    {
        PythonProcesses::awaitStart(process->handle);
    }
    channel->setCapacity(PythonProcesses::BUFFERS_PER_PROCESS * processes.size());
}

void PythonProcessShmSource::close()
{
    std::vector<PythonProcesses::Process> ended;
    for (const auto& process : processes)
    {
        ended.push_back(std::exchange(process->handle, {}));
    }
    PythonProcesses::end(ended);
    /// Unmapped only after the processes ended, though they have their own mapping anyway.
    for (const auto& process : processes)
    {
        if (process->memory != nullptr)
        {
            munmap(process->memory, MEMORY_SIZE);
            process->memory = nullptr;
        }
    }
    processes.clear();
    results.clear();
    unsent.reset();
    spare.reset();
    channel.release();
}

bool PythonProcessShmSource::takeInput()
{
    while (!unsent)
    {
        if (channelEnded)
        {
            return false;
        }
        auto [input, ended] = channel->tryPop();
        if (!input)
        {
            channelEnded = ended;
            return false;
        }
        /// An empty buffer still carries its sequence and chunk number and the watermark.
        if (input->getNumberOfTuples() == 0)
        {
            auto output = zeroCopy ? input : std::nullopt;
            results.push_back({.input = std::move(*input), .output = std::move(output)});
            continue;
        }
        unsent = std::move(input);
    }
    return true;
}

bool PythonProcessShmSource::feedProcesses()
{
    bool sent = false;
    for (auto& process : processes)
    {
        bool sentToProcess = false;
        bool drained = false;
        while (process->inFlight.size() < MAX_IN_FLIGHT)
        {
            if (!takeInput())
            {
                drained = true;
                break;
            }
            if (!(zeroCopy ? sendInPlace(*process, *unsent) : sendCopy(*process, *unsent)))
            {
                break;
            }
            unsent.reset();
            sentToProcess = true;
        }
        /// One wake-up for all the batches the process got in this round.
        if (sentToProcess)
        {
            wakeIfAsleep(*process);
            sent = true;
        }
        if (drained)
        {
            /// The channel has nothing for the other processes either.
            break;
        }
    }
    return sent;
}

std::byte* PythonProcessShmSource::reserveRequest(Process& process, const size_t record)
{
    if (record > RING_SIZE)
    {
        throw UdfExecutionError(
            "A batch of {} bytes does not fit the shared memory of a UDF process, whose rings hold {} bytes", record, RING_SIZE);
    }
    auto* const ring = requestRing(process.memory);
    auto position = process.requestTail % RING_SIZE;
    const auto skip = RING_SIZE - position < record ? RING_SIZE - position : 0;
    const auto head = word(process.memory, Word::REQUEST_HEAD).load(std::memory_order_acquire);
    if (RING_SIZE - (process.requestTail - head) < skip + record)
    {
        return nullptr;
    }
    if (skip > 0)
    {
        store(ring + position, WRAP);
        process.requestTail += skip;
        position = 0;
    }
    return ring + position;
}

void PythonProcessShmSource::publishRequest(Process& process, const size_t record)
{
    process.requestTail += record;
    word(process.memory, Word::REQUEST_TAIL).store(process.requestTail, std::memory_order_release);
}

void PythonProcessShmSource::wakeIfAsleep(Process& process)
{
    /// Orders the publication before the check, against the process setting its flag and checking the tail in the opposite order.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (auto sleeps = word(process.memory, Word::PROCESS_SLEEPS); sleeps.load(std::memory_order_relaxed) != 0)
    {
        sleeps.store(0, std::memory_order_relaxed);
        wake(process.handle);
    }
}

bool PythonProcessShmSource::sendCopy(Process& process, const TupleBuffer& input)
{
    const auto buffers = arrowBuffersOf(input, *layout);
    const auto bodySize = rawBodySize(buffers);
    const auto record = REQUEST_HEADER_SIZE + bodySize;
    auto* cursor = reserveRequest(process, record);
    if (cursor == nullptr)
    {
        return false;
    }

    /// [record size][rows][body size], then per field its buffers as [size][bytes, padded].
    store(cursor, record);
    store(cursor + sizeof(uint64_t), input.getNumberOfTuples());
    store(cursor + (2 * sizeof(uint64_t)), bodySize);
    cursor += REQUEST_HEADER_SIZE;
    for (const auto& buffer : buffers)
    {
        store(cursor, buffer.size());
        if (!buffer.empty())
        {
            std::memcpy(cursor + sizeof(uint64_t), buffer.data(), buffer.size());
        }
        cursor += sizeof(uint64_t) + paddedSize(buffer.size());
    }
    publishRequest(process, record);
    process.inFlight.push_back({.input = input});
    return true;
}

bool PythonProcessShmSource::sendInPlace(Process& process, const TupleBuffer& input)
{
    /// The process reads the header of the buffer itself, so it only needs to know where the buffer and its children are. The results
    /// go into the columns the emit before the sink left for them, which are in the buffer itself.
    const auto buffer = input.getAvailableMemoryArea();
    if (!pool->contains(buffer.data(), buffer.size()))
    {
        throw UdfExecutionError("A batch for a UDF process is outside the shared buffer pool");
    }
    /// Only the children outside the pool travel in the ring, such as large VARSIZED values in unpooled buffers.
    const auto numberOfChildren = input.getNumberOfChildBuffers();
    std::vector<std::span<const std::byte>> children;
    children.reserve(numberOfChildren);
    uint64_t inlineSize = 0;
    for (uint32_t index = 0; index < numberOfChildren; ++index)
    {
        /// The input keeps its children alive.
        const auto& child = children.emplace_back(input.withChildBuffer(
            ChildBufferIndex{index}, [](const TupleBufferView view) { return std::span<const std::byte>{view.getAvailableMemoryArea()}; }));
        inlineSize += pool->contains(child.data(), child.size()) ? 0 : paddedSize(child.size());
    }
    const auto record = IN_PLACE_REQUEST_HEADER_SIZE + (IN_PLACE_REQUEST_ENTRY_SIZE * children.size()) + inlineSize;
    auto* const cursor = reserveRequest(process, record);
    if (cursor == nullptr)
    {
        return false;
    }
    /// VARSIZED results need room for their values, a buffer of the pool if there is one free. A process gets them ahead of time, so
    /// that it rarely runs out before the source learns that it moved on to the next.
    std::optional<TupleBuffer> valueArea;
    if (varSizedResults && process.valueAreas.size() < MAX_VALUE_AREAS)
    {
        valueArea = bufferProvider->getBufferNoBlocking();
        if (valueArea && !pool->contains(valueArea->getAvailableMemoryArea().data(), valueArea->getBufferSize()))
        {
            valueArea.reset();
        }
    }

    store(cursor, record);
    store(cursor + sizeof(uint64_t), input.getNumberOfTuples());
    store(cursor + (2 * sizeof(uint64_t)), children.size());
    store(cursor + (3 * sizeof(uint64_t)), valueArea ? static_cast<uint64_t>(valueArea->getAvailableMemoryArea().data() - pool->base) : 0);
    store(cursor + (4 * sizeof(uint64_t)), valueArea ? valueArea->getBufferSize() : 0);
    store(cursor + (5 * sizeof(uint64_t)), static_cast<uint64_t>(buffer.data() - pool->base));
    if (valueArea)
    {
        process.valueAreas.push_back(std::move(*valueArea));
    }
    auto* entry = cursor + IN_PLACE_REQUEST_HEADER_SIZE;
    auto* const inlineArea = entry + (IN_PLACE_REQUEST_ENTRY_SIZE * children.size());
    uint64_t inlineOffset = 0;
    for (const auto& child : children)
    {
        if (pool->contains(child.data(), child.size()))
        {
            store(entry, static_cast<uint64_t>(child.data() - pool->base));
        }
        else
        {
            store(entry, INLINE | inlineOffset);
            std::memcpy(inlineArea + inlineOffset, child.data(), child.size());
            inlineOffset += paddedSize(child.size());
        }
        store(entry + sizeof(uint64_t), child.size());
        entry += IN_PLACE_REQUEST_ENTRY_SIZE;
    }
    publishRequest(process, record);
    process.inFlight.push_back({.input = input});
    return true;
}

bool PythonProcessShmSource::receiveAnswers(Process& process, const std::stop_token& stopToken)
{
    auto* const ring = answerRing(process.memory);
    bool received = false;
    while (true)
    {
        const auto tail = word(process.memory, Word::ANSWER_TAIL).load(std::memory_order_acquire);
        if (process.answerHead == tail)
        {
            return received;
        }
        const auto position = process.answerHead % RING_SIZE;
        const auto* const record = ring + position;
        const auto recordSize = load(record);
        if (recordSize == WRAP)
        {
            process.answerHead += RING_SIZE - position;
            word(process.memory, Word::ANSWER_HEAD).store(process.answerHead, std::memory_order_release);
            continue;
        }
        if (recordSize < ANSWER_HEADER_SIZE || recordSize > RING_SIZE - position || recordSize % sizeof(uint64_t) != 0
            || recordSize > tail - process.answerHead)
        {
            throw UdfExecutionError("The UDF process wrote an answer of an invalid size {}", recordSize);
        }
        const auto status = load(record + sizeof(uint64_t));
        const auto count = load(record + (2 * sizeof(uint64_t)));
        if (status != 1)
        {
            const auto length = std::min<uint64_t>(count, recordSize - ANSWER_HEADER_SIZE);
            throw UdfExecutionError(
                "Python UDF failed: {}", std::string_view{reinterpret_cast<const char*>(record + ANSWER_HEADER_SIZE), length}); /// NOLINT
        }
        if (process.inFlight.empty() || count != process.inFlight.front().input.getNumberOfTuples())
        {
            throw UdfExecutionError(
                "The UDF process returned {} rows for a batch of {}",
                count,
                process.inFlight.empty() ? 0 : process.inFlight.front().input.getNumberOfTuples());
        }

        auto& flight = process.inFlight.front();
        std::optional<TupleBuffer> output;
        if (zeroCopy)
        {
            /// The results are in the input, which goes on as the output.
            receiveInPlace(process, flight, std::span{record, recordSize});
            output = flight.input;
        }
        else
        {
            output = std::exchange(spare, std::nullopt);
            while (!output)
            {
                if (stopToken.stop_requested())
                {
                    return received;
                }
                output = bufferProvider->getBufferWithTimeout(PythonProcesses::BUFFER_WAIT);
            }
            receiveCopy(flight, std::span{record, recordSize}, *output);
        }

        results.push_back({.input = std::move(flight.input), .output = std::move(output)});
        process.inFlight.pop_front();
        process.answerHead += recordSize;
        word(process.memory, Word::ANSWER_HEAD).store(process.answerHead, std::memory_order_release);
        received = true;
    }
}

void PythonProcessShmSource::receiveCopy(const InFlight& flight, const std::span<const std::byte> record, TupleBuffer& output) const
{
    const auto bodySize = load(record.data() + (3 * sizeof(uint64_t)));
    if (bodySize > record.size() - ANSWER_HEADER_SIZE)
    {
        throw UdfExecutionError("The UDF process wrote an answer whose body of {} bytes exceeds it", bodySize);
    }
    const auto bodyLocation = PythonProcesses::placeBody(output, bodySize, *bufferProvider, *layout);
    auto* const target = const_cast<std::byte*>(ArrowBufferRef::resolve(output, bodyLocation)); /// NOLINT
    std::memcpy(target, record.data() + ANSWER_HEADER_SIZE, bodySize);
    importRawBody(target, bodySize, flight.input.getNumberOfTuples(), bodyLocation, output, *layout, *arrowSchema.get());
}

void PythonProcessShmSource::receiveInPlace(Process& process, InFlight& flight, const std::span<const std::byte> record) const
{
    const auto count = load(record.data() + (3 * sizeof(uint64_t)));
    if (count != calls.size() || ANSWER_HEADER_SIZE + (count * IN_PLACE_ANSWER_ENTRY_SIZE) > record.size())
    {
        throw UdfExecutionError("The UDF process answered with {} results for {} calls", count, calls.size());
    }
    const auto* const entries = record.data() + ANSWER_HEADER_SIZE;
    const auto inlineArea = record.subspan(ANSWER_HEADER_SIZE + (count * IN_PLACE_ANSWER_ENTRY_SIZE));
    const auto rows = flight.input.getNumberOfTuples();
    auto& output = flight.input;
    using Location = ArrowBufferRef::Location;
    using Region = ArrowBufferRef::Region;
    constexpr Location none{.buffer = Location::NO_BUFFER, .offset = 0};

    /// The value areas that hold values of this batch become its children, each once.
    std::vector<std::pair<const std::byte*, uint32_t>> children;
    const auto attach = [&](const TupleBuffer& child)
    {
        const auto* const key = child.getAvailableMemoryArea().data();
        if (const auto known = std::ranges::find(children, key, &std::pair<const std::byte*, uint32_t>::first); known != children.end())
        {
            return known->second;
        }
        auto handle = child;
        const auto index = output.storeChildBuffer(handle).getRawValue() + 1;
        children.emplace_back(key, index);
        return index;
    };
    /// The process writes the value areas in the order it got them, so the ones before the area of these values are full.
    const auto inValueArea = [&](const uint64_t offset, const uint64_t length) -> Location
    {
        while (!process.valueAreas.empty())
        {
            const auto& area = process.valueAreas.front();
            const auto start = static_cast<uint64_t>(area.getAvailableMemoryArea().data() - pool->base);
            if (offset >= start && offset - start <= area.getBufferSize() && length <= area.getBufferSize() - (offset - start))
            {
                return {.buffer = attach(area), .offset = static_cast<uint32_t>(offset - start)};
            }
            process.valueAreas.pop_front();
        }
        throw UdfExecutionError("The UDF process wrote {} bytes at {} outside of its value areas", length, offset);
    };

    for (size_t index = 0; index < count; ++index)
    {
        const auto* const entry = entries + (index * IN_PLACE_ANSWER_ENTRY_SIZE);
        const auto nulls = load(entry);
        const auto where = static_cast<Where>(load(entry + sizeof(uint64_t)));
        const auto offset = load(entry + (2 * sizeof(uint64_t)));
        const auto length = load(entry + (3 * sizeof(uint64_t)));
        const auto field = calls[index].resultField;
        /// The emit left the bitmap all NULL, which the process overwrote only if there are NULLs.
        if (nulls == 0)
        {
            layout->setLocation(output, field, Region::VALIDITY, none);
        }
        if (layout->getFields()[field].type.type != DataType::Type::VARSIZED)
        {
            if (where != Where::NONE)
            {
                throw UdfExecutionError("The UDF process returned VARSIZED values for a result of a fixed size");
            }
            continue;
        }

        /// The process wrote the offsets into the column, which must stay within the values.
        const auto dataLocation = layout->getLocation(output, field, Region::DATA);
        INVARIANT(dataLocation.buffer == Location::MAIN_BUFFER, "The emit leaves the offsets of a result in the buffer itself");
        const auto* const offsets = ArrowBufferRef::resolve(output, dataLocation);
        if (length > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()))
        {
            throw UdfExecutionError("The UDF process returned {} bytes of VARSIZED values for {} rows", length, rows);
        }
        int32_t previous = 0;
        for (uint64_t row = 0; row <= rows; ++row)
        {
            int32_t current = 0;
            std::memcpy(&current, offsets + (row * sizeof(int32_t)), sizeof(int32_t));
            if (current < previous || (row == 0 && current != 0))
            {
                throw UdfExecutionError("The UDF process returned VARSIZED values with invalid offsets");
            }
            previous = current;
        }
        if (static_cast<uint64_t>(previous) != length)
        {
            throw UdfExecutionError("The UDF process returned {} bytes of VARSIZED values, but its offsets end at {}", length, previous);
        }

        Location values = none;
        switch (where)
        {
            case Where::NONE:
                if (length != 0)
                {
                    throw UdfExecutionError("The UDF process returned {} bytes of VARSIZED values without saying where", length);
                }
                break;
            case Where::VALUE_AREA:
                values = inValueArea(offset, length);
                break;
            case Where::INLINE: {
                /// The values did not fit into a value area, so they came in the answer.
                if (offset > inlineArea.size() || length > inlineArea.size() - offset)
                {
                    throw UdfExecutionError("The UDF process answered with values beyond its answer");
                }
                auto child = length <= bufferProvider->getBufferSize() ? std::optional{bufferProvider->getBufferBlocking()}
                                                                       : bufferProvider->getUnpooledBuffer(length);
                if (!child)
                {
                    throw CannotAllocateBuffer("Cannot allocate unpooled buffer of size {}", length);
                }
                std::memcpy(child->getAvailableMemoryArea().data(), inlineArea.data() + offset, length);
                values = {.buffer = output.storeChildBuffer(*child).getRawValue() + 1, .offset = 0};
                break;
            }
            default:
                throw UdfExecutionError("The UDF process answered with values of an unknown kind {}", fmt::underlying(where));
        }
        layout->setLocation(output, field, Region::VALUES, values);
    }
}

void PythonProcessShmSource::sleep()
{
    for (const auto& process : processes)
    {
        word(process->memory, Word::WORKER_SLEEPS).store(1, std::memory_order_relaxed);
    }
    /// Orders the flags before the checks, against a process publishing an answer and checking the flag in the opposite order.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const bool answered = std::ranges::any_of(
        processes,
        [](const auto& process)
        { return word(process->memory, Word::ANSWER_TAIL).load(std::memory_order_acquire) != process->answerHead; });
    if (!answered)
    {
        std::vector<pollfd> descriptors;
        descriptors.reserve(processes.size() + 1);
        for (const auto& process : processes)
        {
            descriptors.push_back({.fd = process->handle.socket, .events = POLLIN, .revents = 0});
        }
        if (!channelEnded)
        {
            descriptors.push_back({.fd = channel->notifier(), .events = POLLIN, .revents = 0});
        }
        if (poll(descriptors.data(), descriptors.size(), SLEEP_MS) < 0 && errno != EINTR)
        {
            throw UdfExecutionError("Failed to wait for the UDF processes: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
        }
        for (size_t index = 0; index < processes.size(); ++index)
        {
            if (descriptors[index].revents == 0)
            {
                continue;
            }
            std::array<std::byte, 256> wakeUps{};
            const auto received = recv(processes[index]->handle.socket, wakeUps.data(), wakeUps.size(), MSG_DONTWAIT);
            if (received == 0)
            {
                throw UdfExecutionError("The UDF process exited unexpectedly");
            }
        }
        if (!channelEnded && descriptors.back().revents != 0)
        {
            uint64_t count = 0;
            [[maybe_unused]] const auto read = ::read(channel->notifier(), &count, sizeof(count));
        }
    }
    for (const auto& process : processes)
    {
        word(process->memory, Word::WORKER_SLEEPS).store(0, std::memory_order_relaxed);
    }
}

std::optional<PythonProcessShmSource::Result> PythonProcessShmSource::nextResult(const std::stop_token& stopToken)
{
    std::optional<std::chrono::steady_clock::time_point> idleSince;
    while (results.empty())
    {
        bool progressed = feedProcesses();
        /// Answers are taken only once the ones taken before have been passed on, so that they wait in the rings, where they hold no
        /// buffer and keep their processes from being sent more.
        /// Round robin, so that every process gets its answers taken.
        for (size_t checked = 0; checked < processes.size() && results.empty(); ++checked)
        {
            progressed = receiveAnswers(*processes[nextProcess], stopToken) || progressed;
            nextProcess = (nextProcess + 1) % processes.size();
        }
        if (!results.empty())
        {
            break;
        }
        const bool busy = unsent || std::ranges::any_of(processes, [](const auto& process) { return !process->inFlight.empty(); });
        if (stopToken.stop_requested() || (channelEnded && !busy))
        {
            return std::nullopt;
        }
        if (progressed)
        {
            idleSince.reset();
            continue;
        }
        const auto now = std::chrono::steady_clock::now();
        if (!idleSince)
        {
            idleSince = now;
        }
        if (now - *idleSince < SPIN_DURATION)
        {
            spinPause();
            continue;
        }
        sleep();
        idleSince.reset();
    }
    auto result = std::move(results.front());
    results.pop_front();
    return result;
}

std::optional<TupleBuffer> PythonProcessShmSource::nextBuffer(const std::stop_token& stopToken)
{
    /// The answers are in the batches themselves, which keep their metadata.
    auto result = nextResult(stopToken);
    if (!result)
    {
        return std::nullopt;
    }
    return std::move(result->input);
}

Source::FillTupleBufferResult PythonProcessShmSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    /// The source loop got this buffer from the pool for the source to fill; the next answer is copied into it.
    if (!spare)
    {
        spare = tupleBuffer;
    }
    auto result = nextResult(stopToken);
    if (!result)
    {
        return FillTupleBufferResult::eos();
    }
    auto& [input, output] = *result;
    if (output)
    {
        tupleBuffer = std::move(*output);
    }
    else
    {
        /// The buffer goes out as it is, so no answer may be copied into it, but one may already be waiting in it.
        const auto isOurs = [&](const std::optional<TupleBuffer>& buffer)
        { return buffer && buffer->getAvailableMemoryArea().data() == tupleBuffer.getAvailableMemoryArea().data(); };
        if (isOurs(spare))
        {
            spare.reset();
        }
        else if (std::ranges::any_of(results, [&](const Result& waiting) { return isOurs(waiting.output); }))
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
