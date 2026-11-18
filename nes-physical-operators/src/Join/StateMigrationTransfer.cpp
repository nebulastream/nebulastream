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

#include <Join/StateMigrationTransfer.hpp>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <ErrorHandling.hpp>
#include <NetworkBindings.hpp>

namespace NES::StateMigrationTransfer
{
namespace
{
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(1);

/// Zero selects the defaults of the network services.
constexpr NetworkServiceOptions DEFAULT_OPTIONS{
    .sender_queue_size = 0, .max_pending_acks = 0, .receiver_queue_size = 0, .sender_io_threads = 0, .receiver_io_threads = 0};
}

void send(
    const std::string& channel,
    const std::string& senderDataEndpoint,
    const std::string& receiverDataEndpoint,
    const std::vector<TupleBuffer>& state,
    const std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const auto checkDeadline = [&](const std::string_view phase)
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            throw CannotWriteToSink("The state migration over channel {} could not {} within {}", channel, phase, timeout);
        }
    };

    auto server = sender_instance(senderDataEndpoint);
    auto senderChannel = register_sender_channel(*server, receiverDataEndpoint, rust::String(channel), DEFAULT_OPTIONS);
    uint64_t sequenceNumber = INITIAL_SEQ_NUMBER.getRawValue();
    for (const auto& buffer : state)
    {
        const SerializedTupleBufferHeader metadata{
            .sequence_number = sequenceNumber++,
            .origin_id = INITIAL_ORIGIN_ID.getRawValue(),
            .chunk_number = INITIAL_CHUNK_NUMBER.getRawValue(),
            .number_of_tuples = buffer.getNumberOfTuples(),
            .watermark = 0,
            .last_chunk = true};
        const auto payload = buffer.getAvailableMemoryArea<const uint8_t>().subspan(0, buffer.getNumberOfTuples());
        for (auto sent = false; not sent;)
        {
            switch (send_buffer(*senderChannel, metadata, rust::Slice(payload), rust::Slice<const rust::Slice<const uint8_t>>()))
            {
                case SendResult::Ok:
                    sent = true;
                    break;
                case SendResult::Full:
                    checkDeadline("send the state");
                    std::this_thread::sleep_for(POLL_INTERVAL);
                    break;
                case SendResult::Closed:
                case SendResult::Detached:
                    close_sender_channel(std::move(senderChannel));
                    throw CannotWriteToSink("The receiver of the state migration over channel {} left", channel);
            }
        }
    }
    for (auto flushed = false; not flushed;)
    {
        switch (flush_sender_channel(*senderChannel))
        {
            case FlushResult::Flushed:
                flushed = true;
                break;
            case FlushResult::Pending:
                checkDeadline("flush the state");
                std::this_thread::sleep_for(POLL_INTERVAL);
                break;
            case FlushResult::Failed:
            case FlushResult::Detached:
                close_sender_channel(std::move(senderChannel));
                throw CannotWriteToSink("The receiver of the state migration over channel {} left", channel);
        }
    }
    close_sender_channel(std::move(senderChannel));
    NES_DEBUG("Sent {} state buffers over channel {} to {}", state.size(), channel, receiverDataEndpoint);
}

std::vector<TupleBuffer> receive(
    const std::string& channel,
    const std::string& receiverDataEndpoint,
    AbstractBufferProvider& bufferProvider,
    const std::chrono::milliseconds timeout)
{
    auto server = receiver_instance(receiverDataEndpoint);
    auto receiverChannel = register_receiver_channel(*server, rust::String(channel), DEFAULT_OPTIONS);

    /// receive_buffer blocks; a watchdog interrupts it once the timeout expires.
    std::mutex mutex;
    std::condition_variable done;
    bool finished = false;
    bool timedOut = false;
    std::jthread watchdog(
        [&]
        {
            std::unique_lock lock(mutex);
            if (not done.wait_for(lock, timeout, [&] { return finished; }))
            {
                timedOut = true;
                interrupt_receive(*receiverChannel);
            }
        });

    std::vector<TupleBuffer> state;
    for (auto closed = false; not closed;)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        TupleBufferBuilder builder(buffer, bufferProvider);
        const auto result = receive_buffer(*receiverChannel, builder);
        switch (result.kind)
        {
            case ReceiveKind::Buffer:
                INVARIANT(
                    buffer.getSequenceNumber().getRawValue() == INITIAL_SEQ_NUMBER.getRawValue() + state.size(),
                    "State buffers of channel {} arrive in order",
                    channel);
                state.emplace_back(std::move(buffer));
                break;
            case ReceiveKind::Marker:
            case ReceiveKind::Closed:
                closed = true;
                break;
        }
    }
    {
        const std::scoped_lock lock(mutex);
        finished = true;
    }
    done.notify_all();
    watchdog.join();
    close_receiver_channel(std::move(receiverChannel));
    if (timedOut)
    {
        throw CannotOpenSource("The state migration over channel {} did not finish within {}", channel, timeout);
    }
    NES_DEBUG("Received {} state buffers over channel {}", state.size(), channel);
    return state;
}

}
