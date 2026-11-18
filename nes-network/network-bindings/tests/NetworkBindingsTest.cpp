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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 1024;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 16;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;

/// Behaves like the wrapped provider, but cannot allocate unpooled buffers, which makes TupleBufferBuilder::addChildBuffer throw.
class NoUnpooledBufferProvider final : public NES::AbstractBufferProvider
{
public:
    explicit NoUnpooledBufferProvider(std::shared_ptr<NES::AbstractBufferProvider> inner) : inner(std::move(inner)) { }

    NES::BufferManagerType getBufferManagerType() const override { return inner->getBufferManagerType(); }
    size_t getBufferSize() const override { return inner->getBufferSize(); }
    size_t getNumOfPooledBuffers() const override { return inner->getNumOfPooledBuffers(); }
    size_t getNumOfUnpooledBuffers() const override { return inner->getNumOfUnpooledBuffers(); }
    NES::TupleBuffer getBufferBlocking() override { return inner->getBufferBlocking(); }
    std::optional<NES::TupleBuffer> getBufferNoBlocking() override { return inner->getBufferNoBlocking(); }
    std::optional<NES::TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout) override
    {
        return inner->getBufferWithTimeout(timeout);
    }
    std::optional<NES::TupleBuffer> getUnpooledBuffer(size_t) override { return std::nullopt; }

private:
    std::shared_ptr<NES::AbstractBufferProvider> inner;
};
}

TEST(NetworkBindingsTest, ReceiveBufferReturnsErrorWhenChildBufferCannotBeAllocated)
{
    const std::string connectionAddr = "tcp://127.0.0.1:18090";
    const std::string channelId = "child-buffer-allocation-failure";
    const NetworkServiceOptions options{
        .sender_queue_size = 4, .max_pending_acks = 4, .receiver_queue_size = 4, .sender_io_threads = 1, .receiver_io_threads = 1};

    enable_memcom();
    init_receiver_service(rust::String(connectionAddr), rust::String("test-worker"), options);
    init_sender_service(rust::String(connectionAddr), rust::String("test-worker"), options);

    auto receiverService = receiver_instance(rust::String(connectionAddr));
    auto receiverChannel = register_receiver_channel(*receiverService, rust::String(channelId), options);
    auto senderService = sender_instance(rust::String(connectionAddr));
    auto senderChannel = register_sender_channel(*senderService, rust::String(connectionAddr), rust::String(channelId), options);

    const SerializedTupleBufferHeader header{
        .sequence_number = 1, .origin_id = 1, .chunk_number = 1, .number_of_tuples = 1, .watermark = 1, .last_chunk = true};
    constexpr std::array<uint8_t, 8> data{};
    constexpr std::array<uint8_t, 8> child{};
    const std::array<rust::Slice<const uint8_t>, 1> children{rust::Slice<const uint8_t>(child.data(), child.size())};
    ASSERT_EQ(
        send_buffer(
            *senderChannel,
            header,
            rust::Slice<const uint8_t>(data.data(), data.size()),
            rust::Slice<const rust::Slice<const uint8_t>>(children.data(), children.size())),
        SendResult::Ok);

    auto bufferManager = NES::BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        NES::BufferAlignment{64},
        POOLED_BUFFER_SIZE,
        std::make_shared<NES::NesDefaultMemoryAllocator>());
    NoUnpooledBufferProvider provider(bufferManager);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    TupleBufferBuilder builder(tupleBuffer, provider);

    EXPECT_THROW(receive_buffer(*receiverChannel, builder), rust::Error);

    close_sender_channel(std::move(senderChannel));
    close_receiver_channel(std::move(receiverChannel));
}
