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



#include <cstddef>
#include <cstdint>
#include <future>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Runtime/BufferPrimitives.hpp>
#include <Runtime/DataSegment.hpp>

namespace NES::Memory::detail
{
}
/// Check: not zero and `v` has got no 1 in common with `v - 1`.
/// Making use of short-circuit evaluation here because otherwise v-1 might be an underflow.
/// TODO: switch to std::ispow2 when we use C++2a.
template <std::size_t v>
static constexpr bool ispow2 = (!!v) && !(v & (v - 1));

namespace NES::Memory
{
class FloatingBuffer;

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

class PinnedBuffer
{
    /// Utilize the wrapped-memory constructor
    friend class BufferManager;
    friend class FixedSizeBufferPool;
    friend class LocalBufferPool;
    friend class FloatingBuffer;

    //[[nodiscard]] explicit PinnedBuffer(detail::BufferControlBlock* controlBlock) noexcept;
    [[nodiscard]] explicit PinnedBuffer(
        detail::BufferControlBlock* controlBlock, detail::DataSegment<detail::InMemoryLocation> segment, detail::ChildOrMainDataKey childOrMainData) noexcept;

public:


    /// @brief Default constructor creates an empty wrapper around nullptr without controlBlock (nullptr) and size 0.
    [[nodiscard]] PinnedBuffer() noexcept = default;

    /**
     * @brief Interprets the void* as a pointer to the content of tuple buffer
     * @note if bufferPointer is not pointing to the begin of an data buffer the behavior of this function is undefined.
     * @note also, make sure that the buffers data is indeed in memory and not spilled
     * @param bufferPointer
     * @return TupleBuffer
     */
    // [[maybe_unused]] static PinnedBuffer reinterpretAsTupleBuffer(void* bufferPointer);

    /// @brief Copy constructor: Increase the reference count associated to the control buffer.
    [[nodiscard]] PinnedBuffer(PinnedBuffer const& other) noexcept;

    /// @brief Move constructor: Steal the resources from `other`. This does not affect the reference count.
    /// @dev In this constructor, `other` is cleared, because otherwise its destructor would release its old memory.
    [[nodiscard]] PinnedBuffer(PinnedBuffer&& other) noexcept : controlBlock(other.controlBlock), dataSegment(other.dataSegment), childOrMainData(other.childOrMainData)
    {
        other.controlBlock = nullptr;
        other.dataSegment = detail::DataSegment{detail::InMemoryLocation{nullptr}, 0};
        other.childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
    }

    // /// @brief repins a buffer that might got spilled. Can block and repinning should be done from one thread only.
    // [[nodiscard]] explicit PinnedBuffer(FloatingBuffer&& other) noexcept;

    /// @brief Assign the `other` resource to this TupleBuffer; increase and decrease reference count if necessary.
    PinnedBuffer& operator=(PinnedBuffer const& other) noexcept;

    /// @brief Assign the `other` resource to this TupleBuffer; Might release the resource this currently points to.
    PinnedBuffer& operator=(PinnedBuffer&& other) noexcept;

    /// @brief Delete address-of operator to make it harder to circumvent reference counting mechanism with an l-value.
    PinnedBuffer* operator&() = delete;

    /// @brief Return if this is not valid.
    [[nodiscard]] auto operator!() const noexcept -> bool { return dataSegment.getLocation().getPtr() == nullptr; }

    /// @brief release the resource if necessary.
    ~PinnedBuffer() noexcept;

    /// @brief Swap `lhs` and `rhs`.
    /// @dev Accessible via ADL in an unqualified call.
    friend void swap(PinnedBuffer& lhs, PinnedBuffer& rhs) noexcept;

    /// @brief Increases the internal reference counter by one and return this.
    PinnedBuffer& retain() noexcept;

    /// @brief Decrease internal reference counter by one and release the resource when the reference count reaches 0.
    void release() noexcept;

    int8_t* getBuffer() noexcept;

    /// @brief return the TupleBuffer's content as pointer to `T`.
    template <typename T = int8_t>
    T* getBuffer() noexcept
    {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<T*>(dataSegment.getLocation().getPtr());
    }

    /// @brief return the TupleBuffer's content as pointer to `T`.
    template <typename T = int8_t>
    const T* getBuffer() const noexcept
    {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<const T*>(dataSegment.getLocation().getPtr());
    }

    [[nodiscard]] uint32_t getReferenceCounter() const noexcept;

    [[nodiscard]] bool isValid() const noexcept;


    /// @brief Print the buffer's address.
    /// @dev TODO: consider changing the reinterpret_cast to  std::bit_cast in C++2a if possible.
    friend std::ostream& operator<<(std::ostream& os, const PinnedBuffer& buff) noexcept;

    /// @brief get the buffer's size.
    [[nodiscard]] uint64_t getBufferSize() const noexcept;

    /// @brief get the number of tuples stored.
    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept;

    /// @brief set the number of tuples stored.
    void setNumberOfTuples(uint64_t numberOfTuples) const noexcept;

    /// @brief get the watermark as a timestamp
    [[nodiscard]] Runtime::Timestamp getWatermark() const noexcept;

    /// @brief set the watermark from a timestamp
    void setWatermark(Runtime::Timestamp value) noexcept;

    /// @brief get the creation timestamp in milliseconds
    [[nodiscard]] Runtime::Timestamp getCreationTimestampInMS() const noexcept;

    /// @brief set the sequence number
    void setSequenceNumber(SequenceNumber sequenceNumber) noexcept;

    [[nodiscard]] std::string getSequenceDataAsString() const noexcept;

    /// @brief get the sequence number
    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept;

    /// @brief set the chunk number
    void setChunkNumber(ChunkNumber chunkNumber) noexcept;

    /// @brief get the chunk number
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept;

    /// @brief set if this is the last chunk of a sequence number
    void setLastChunk(bool isLastChunk) noexcept;

    /// @brief retrieves if this is the last chunk
    [[nodiscard]] bool isLastChunk() const noexcept;

    /// @brief set the creation timestamp in milliseconds
    void setCreationTimestampInMS(Runtime::Timestamp value) noexcept;

    ///@brief get the buffer's origin id (the operator id that creates this buffer).
    [[nodiscard]] OriginId getOriginId() const noexcept;

    ///@brief set the buffer's origin id (the operator id that creates this buffer).
    void setOriginId(OriginId id) noexcept;

    ///@brief attach the content of a buffer to the parent.
    ///Invalidates the passed PinnedBuffer.
    ///@return a tuple buffer to the same data, but as a child of this buffer
    [[nodiscard]] std::optional<PinnedBuffer> storeReturnAsChildBuffer(PinnedBuffer&& buffer) const noexcept;

    std::optional<ChildKey> storeReturnChildIndex(PinnedBuffer&& other) const noexcept;

    [[nodiscard]] std::optional<PinnedBuffer> loadChildBuffer(const int8_t* ptr, uint32_t size) const noexcept;


    ///@brief retrieve a child tuple buffer via its NestedTupleBufferKey
    [[nodiscard]] std::optional<PinnedBuffer> loadChildBuffer(ChildKey bufferIndex) const;

    // [[nodiscard]] bool deleteChild(PinnedBuffer&& child) noexcept;
    // [[nodiscard]] bool deleteChild(const int8_t* oldbuffer, uint32_t size) const noexcept;

    bool stealChild(const PinnedBuffer* oldParent, const int8_t* child, uint32_t size) const;

    [[nodiscard]] uint32_t getNumberOfChildrenBuffer() const noexcept;


private:
    /**
     * @brief returns the control block of the buffer USE THIS WITH CAUTION!
     */
    [[nodiscard]] detail::BufferControlBlock* getControlBlock() const { return controlBlock; }

    detail::BufferControlBlock* controlBlock = nullptr;
    detail::DataSegment<detail::InMemoryLocation> dataSegment;
    ///If == 0, then this floating buffer refers to the main data segment stored in the BCB.
    ///If == 1, then its unknown what this refer to, please search throught the children vector in the BCB.
    ///If > 1, then childOrMainData - 2 is the index of the child buffer in the BCB that this floating buffer belongs to.
    detail::ChildOrMainDataKey childOrMainData = detail::ChildOrMainDataKey::UNKNOWN();
};


/**
 * @brief This method determines the control block based on the ptr to the data region and decrements the reference counter.
 * @param bufferPointer pointer to the data region of an buffer.
 */
[[maybe_unused]] bool recycleTupleBuffer(void* bufferPointer);

/**
 * @brief Allocates an object of T in the tuple buffer.
 * Set the number of tuples to one.
 * @tparam T
 * @param buffer
 * @return T+
 */
template <typename T>
T* allocateWithin(PinnedBuffer& buffer)
{
    auto ptr = new (buffer.getBuffer()) T();
    buffer.setNumberOfTuples(1);
    return ptr;
};

//For now I'm not sure if I'd want to try to fit this into std::future, wait is not const
class FuturePinnedBuffer
{
private:
    std::promise<detail::DataSegment<detail::InMemoryLocation>> futureInMemorySegment;

public:
    void wait();
};


}
