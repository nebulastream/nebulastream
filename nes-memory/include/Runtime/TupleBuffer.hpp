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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/VariableSizedAccess.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
class UnpooledChunksManager;
}

namespace NES
{
namespace detail
{
class BufferControlBlock;
class MemorySegment;
}

/**
 * @brief The TupleBuffer allows Runtime components to access memory to store records in a reference-counted and
 * thread-safe manner.
 *
 * The purpose of the TupleBuffer is to zero memory allocations and enable batching.
 * In order to zero the memory allocation, a BufferManager keeps a fixed set of fixed-size buffers that it hands out to
 * components.
 * A TupleBuffer's content is automatically recycled or deleted once its reference count reaches zero.
 *
 * Prefer passing the TupleBuffer by reference whenever possible, pass the TupleBuffer to another thread by value.
 *
 * Important note: when a component is done with a TupleBuffer, it must be released. Not returning a TupleBuffer will
 * result in a Runtime error that the BufferManager will raise by the termination of the NES program.
 *
 * A TupleBuffer may store one or more child/nested TupleBuffer. As soon as a TupleBuffer is attached to a parent,
 * it loses ownership of its internal MemorySegment, whose lifecycle is linked to the lifecycle of the parent.
 * This means that when the parent TupleBuffer goes out of scope, no child TupleBuffer must be alive in the program.
 * If that occurs, an error is raised.
 *
 * Reminder: this class should be header-only to help inlining
 */

class TupleBuffer
{
    /// Utilize the wrapped-memory constructor
    friend class BufferManager;
    friend class UnpooledChunksManager;
    friend class FixedSizeBufferPool;
    friend class LocalBufferPool;
    friend class detail::MemorySegment;

    [[nodiscard]] explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size) noexcept
        : controlBlock(controlBlock), ptr(ptr), size(size)
    {
        /// nop
    }

public:
    /// @brief Default constructor creates an empty wrapper around nullptr without controlBlock (nullptr) and size 0.
    [[nodiscard]] TupleBuffer() noexcept = default;


    /// @brief Copy constructor: Increase the reference count associated to the control buffer.
    [[nodiscard]] TupleBuffer(const TupleBuffer& other) noexcept;

    /// @brief Move constructor: Steal the resources from `other`. This does not affect the reference count.
    /// @dev In this constructor, `other` is cleared, because otherwise its destructor would release its old memory.
    [[nodiscard]] TupleBuffer(TupleBuffer&& other) noexcept : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size)
    {
        other.controlBlock = nullptr;
        other.ptr = nullptr;
        other.size = 0;
    }

    /// @brief Assign the `other` resource to this TupleBuffer; increase and decrease reference count if necessary.
    TupleBuffer& operator=(const TupleBuffer& other) noexcept;

    /// @brief Assign the `other` resource to this TupleBuffer; Might release the resource this currently points to.
    TupleBuffer& operator=(TupleBuffer&& other) noexcept;

    /// @brief Delete address-of operator to make it harder to circumvent reference counting mechanism with an l-value.
    TupleBuffer* operator&() = delete; /// NOLINT (google-runtime-operator)

    /// @brief Return if this is not valid.
    [[nodiscard]] auto operator!() const noexcept -> bool { return ptr == nullptr; }

    /// @brief release the resource if necessary.
    ~TupleBuffer() noexcept;

    /// @brief Swap `lhs` and `rhs`.
    /// @dev Accessible via ADL in an unqualified call.
    friend void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept;

    /// @brief Increases the internal reference counter by one and return this.
    TupleBuffer& retain() noexcept;

    /// @brief Decrease internal reference counter by one and release the resource when the reference count reaches 0.
    void release() noexcept;

    template <typename T = std::byte>
    requires(std::is_trivially_copyable_v<T>)
    std::span<T> getAvailableMemoryArea() noexcept
    {
        PRECONDITION(reinterpret_cast<std::uintptr_t>(ptr) % alignof(T) == 0, "Bad alignment for type: {}", typeid(T).name());
        return std::span<T>{std::bit_cast<T*>(ptr), size / sizeof(T)};
    }

    template <typename T = std::byte>
    requires(std::is_trivially_copyable_v<T>)
    std::span<const T> getAvailableMemoryArea() const noexcept
    {
        PRECONDITION(reinterpret_cast<std::uintptr_t>(ptr) % alignof(const T) == 0, "Bad alignment for type: {}", typeid(T).name());
        return std::span<const T>{std::bit_cast<const T*>(ptr), size / sizeof(const T)};
    }

    [[nodiscard]] uint32_t getReferenceCounter() const noexcept;

    /// @brief Print the buffer's address.
    /// @dev TODO: consider changing the reinterpret_cast to  std::bit_cast in C++2a if possible.
    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept;

    [[nodiscard]] uint64_t getBufferSize() const noexcept;

    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept;
    void setNumberOfTuples(uint64_t numberOfTuples) const noexcept;

    [[nodiscard]] Timestamp getWatermark() const noexcept;
    void setWatermark(Timestamp value) noexcept;

    [[nodiscard]] Timestamp getCreationTimestampInMS() const noexcept;
    void setSequenceNumber(SequenceNumber sequenceNumber) noexcept;

    [[nodiscard]] std::string getSequenceDataAsString() const noexcept;

    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept;

    void setChunkNumber(ChunkNumber chunkNumber) noexcept;
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept;

    /// @brief set if this is the last chunk of a sequence number
    void setLastChunk(bool isLastChunk) noexcept;

    /// @brief retrieves if this is the last chunk
    [[nodiscard]] bool isLastChunk() const noexcept;

    void setCreationTimestampInMS(Timestamp value) noexcept;

    [[nodiscard]] OriginId getOriginId() const noexcept;
    void setOriginId(OriginId id) noexcept;

    ///@brief attach a child tuple buffer to the parent. the child tuple buffer is then identified via NestedTupleBufferKey
    [[nodiscard]] VariableSizedAccess::Index storeChildBuffer(TupleBuffer& buffer) noexcept;

    ///@brief retrieve a child tuple buffer via its NestedTupleBufferKey
    [[nodiscard]] TupleBuffer loadChildBuffer(VariableSizedAccess::Index bufferIndex) const noexcept;

    [[nodiscard]] uint32_t getNumberOfChildBuffers() const noexcept;

private:
    /**
     * @brief returns the control block of the buffer USE THIS WITH CAUTION!
     */
    [[nodiscard]] detail::BufferControlBlock* getControlBlock() const { return controlBlock; }

    detail::BufferControlBlock* controlBlock = nullptr;
    uint8_t* ptr = nullptr;
    uint32_t size = 0;
};

/**
 * @brief This method determines the control block based on the ptr to the data region and decrements the reference counter.
 * @param bufferPointer pointer to the data region of an buffer.
 */
[[maybe_unused]] bool recycleTupleBuffer(void* bufferPointer);
}
