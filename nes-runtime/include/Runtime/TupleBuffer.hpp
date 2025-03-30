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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_TUPLEBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_TUPLEBUFFER_HPP_

#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Sequencing/SequenceData.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <utility>

/// Check: not zero and `v` has got no 1 in common with `v - 1`.
/// Making use of short-circuit evaluation here because otherwise v-1 might be an underflow.
/// TODO: switch to std::ispow2 when we use C++2a.
template<std::size_t v>
static constexpr bool ispow2 = (!!v) && !(v & (v - 1));

namespace NES::Network::detail {
template<typename>
class NetworkDataSender;
template<typename>
class NetworkEventSender;
}// namespace NES::Network::detail

namespace NES::Runtime {

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

class TupleBuffer {
    /// Utilize the wrapped-memory constructor
    friend class BufferManager;
    friend class FixedSizeBufferPool;
    friend class LocalBufferPool;
    friend class detail::MemorySegment;

    /// Utilize the wrapped-memory constructor and requires direct access to the control block for the ZMQ sink.
    template<typename T>
    friend class NES::Network::detail::NetworkDataSender;
    template<typename T>
    friend class NES::Network::detail::NetworkEventSender;

    [[nodiscard]] constexpr explicit TupleBuffer(detail::BufferControlBlock* controlBlock, uint8_t* ptr, uint32_t size) noexcept
        : controlBlock(controlBlock), ptr(ptr), size(size) {
        // nop
    }

  public:
    ///@brief This is the logical identifier of a child tuple buffer
    using NestedTupleBufferKey = uint32_t;

    ///@brief The initial sequence number of a stream of tuple buffers is 1.
    constexpr static uint64_t INITIAL_SEQUENCE_NUMBER = 1;
    ///@brief When the content of a tuple buffer outgrows the buffer size tuple buffers are chunked to preserve the order of
    ///sequencenumbers (or prevent duplicated). The initial chunk number is 1.
    constexpr static uint64_t INITIAL_CHUNK_NUMBER = 1;

    /// @brief Default constructor creates an empty wrapper around nullptr without controlBlock (nullptr) and size 0.
    [[nodiscard]] constexpr TupleBuffer() noexcept = default;

    /**
     * @brief Interprets the void* as a pointer to the content of tuple buffer
     * @note if bufferPointer is not pointing to the begin of an data buffer the behavior of this function is undefined.
     * @param bufferPointer
     * @return TupleBuffer
     */
    [[maybe_unused]] static TupleBuffer reinterpretAsTupleBuffer(void* bufferPointer);


    /**
     * Wrap an object in a tuple buffer.
     * The tuple buffer retrieves ownership and frees the object when the buffer is released.
     * @param ownership to object
     * @return TupleBuffer
     */
    template<class T>
    [[nodiscard]] static TupleBuffer wrapPtr(std::unique_ptr<T> object) {
        return wrapMemory((uint8_t*) object.release(),
                          sizeof(typename std::unique_ptr<T>::pointer),
                          [](detail::MemorySegment* segment, BufferRecycler*) {
                              delete (typename std::unique_ptr<T>::pointer)(segment->getPointer());
                          });
    }

    /// @brief Copy constructor: Increase the reference count associated to the control buffer.
    [[nodiscard]] constexpr TupleBuffer(TupleBuffer const& other) noexcept
        : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        if (controlBlock != nullptr) {
            controlBlock->retain();
        }
    }

    /// @brief Move constructor: Steal the resources from `other`. This does not affect the reference count.
    /// @dev In this constructor, `other` is cleared, because otherwise its destructor would release its old memory.
    [[nodiscard]] constexpr TupleBuffer(TupleBuffer&& other) noexcept
        : controlBlock(other.controlBlock), ptr(other.ptr), size(other.size) {
        other.controlBlock = nullptr;
        other.ptr = nullptr;
        other.size = 0;
    }

    /// @brief Assign the `other` resource to this TupleBuffer; increase and decrease reference count if necessary.
    TupleBuffer& operator=(TupleBuffer const& other) noexcept {
        if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
            return *this;
        }

        // Override the content of this with those of `other`
        auto* const oldControlBlock = std::exchange(controlBlock, other.controlBlock);
        ptr = other.ptr;
        size = other.size;

        // Update reference counts: If the new and old controlBlocks differ, retain the new one and release the old one.
        if (oldControlBlock != controlBlock) {
            retain();
            if (oldControlBlock) {
                oldControlBlock->release();
            }
        }
        return *this;
    }

    /// @brief Assign the `other` resource to this TupleBuffer; Might release the resource this currently points to.
    inline TupleBuffer& operator=(TupleBuffer&& other) noexcept {
        // Especially for rvalues, the following branch should most likely never be taken if the caller writes
        // reasonable code. Therefore, this branch is considered unlikely.
        if PLACEHOLDER_UNLIKELY (this == std::addressof(other)) {
            return *this;
        }

        // Swap content of this with those of `other` to let the other's destructor take care of releasing the overwritten
        // resource.
        using std::swap;
        swap(*this, other);

        return *this;
    }

    /// @brief Delete address-of operator to make it harder to circumvent reference counting mechanism with an l-value.
    TupleBuffer* operator&() = delete;

    /// @brief Return if this is not valid.
    [[nodiscard]] constexpr auto operator!() const noexcept -> bool { return ptr == nullptr; }

    /// @brief release the resource if necessary.
    inline ~TupleBuffer() noexcept { release(); }

    /// @brief Swap `lhs` and `rhs`.
    /// @dev Accessible via ADL in an unqualified call.
    inline friend void swap(TupleBuffer& lhs, TupleBuffer& rhs) noexcept {
        // Enable ADL to spell out to onlookers how swap should be used.
        using std::swap;

        swap(lhs.ptr, rhs.ptr);
        swap(lhs.size, rhs.size);
        swap(lhs.controlBlock, rhs.controlBlock);
    }

    /// @brief Increases the internal reference counter by one and return this.
    inline TupleBuffer& retain() noexcept {
        if (controlBlock) {
            controlBlock->retain();
        }
        return *this;
    }

    /// @brief Decrease internal reference counter by one and release the resource when the reference count reaches 0.
    inline void release() noexcept {
        if (controlBlock) {
            controlBlock->release();
        }
        controlBlock = nullptr;
        ptr = nullptr;
        size = 0;
    }

    inline uint8_t* getBuffer() noexcept { return getBuffer<uint8_t>(); }

    /// @brief return the TupleBuffer's content as pointer to `T`.
    template<typename T = uint8_t>
    inline T* getBuffer() noexcept {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<T*>(ptr);
    }

    /// @brief return the TupleBuffer's content as pointer to `T`.
    template<typename T = uint8_t>
    inline const T* getBuffer() const noexcept {
        static_assert(alignof(T) <= alignof(std::max_align_t), "Alignment of type T is stricter than allowed.");
        static_assert(ispow2<alignof(T)>);
        return reinterpret_cast<const T*>(ptr);
    }

    [[nodiscard]] inline uint32_t getReferenceCounter() const noexcept {
        return controlBlock ? controlBlock->getReferenceCount() : 0;
    }

    /// @brief Print the buffer's address.
    /// @dev TODO: consider changing the reinterpret_cast to  std::bit_cast in C++2a if possible.
    friend std::ostream& operator<<(std::ostream& os, const TupleBuffer& buff) noexcept {
        return os << reinterpret_cast<std::uintptr_t>(buff.ptr);
    }

    /// @brief get the buffer's size.
    [[nodiscard]] inline uint64_t getBufferSize() const noexcept { return size; }

    /// @brief get the number of tuples stored.
    [[nodiscard]] constexpr uint64_t getNumberOfTuples() const noexcept { return controlBlock->getNumberOfTuples(); }

    /// @brief set the number of tuples stored.
    inline void setNumberOfTuples(uint64_t numberOfTuples) noexcept { controlBlock->setNumberOfTuples(numberOfTuples); }

    /// @brief get the watermark as a timestamp
    [[nodiscard]] constexpr uint64_t getWatermark() const noexcept { return controlBlock->getWatermark(); }

    /// @brief set the watermark from a timestamp
    inline void setWatermark(uint64_t value) noexcept { controlBlock->setWatermark(value); }

    /// @brief get the creation timestamp in milliseconds
    [[nodiscard]] constexpr uint64_t getCreationTimestampInMS() const noexcept { return controlBlock->getCreationTimestamp(); }

    /// @brief set the sequence number
    inline void setSequenceNumber(uint64_t sequenceNumber) noexcept { controlBlock->setSequenceNumber(sequenceNumber); }

    /// @brief set the sequence data, i.e., sequenceNumber, chunkNumber, and lastChunk
    inline void setSequenceData(SequenceData sequenceData) noexcept {
        setSequenceNumber(sequenceData.sequenceNumber);
        setChunkNumber(sequenceData.chunkNumber);
        setLastChunk(sequenceData.lastChunk);
    }

    /// @brief gets the sequence data from this buffer
    inline SequenceData getSequenceData() const noexcept { return {getSequenceNumber(), getChunkNumber(), isLastChunk()}; }

    /// @brief get the sequence number
    [[nodiscard]] constexpr uint64_t getSequenceNumber() const noexcept { return controlBlock->getSequenceNumber(); }

    /// @brief set the sequence number
    inline void setChunkNumber(uint64_t chunkNumber) noexcept { controlBlock->setChunkNumber(chunkNumber); }

    /// @brief get the chunk number
    [[nodiscard]] constexpr uint64_t getChunkNumber() const noexcept { return controlBlock->getChunkNumber(); }

    /// @brief set if this is the last chunk of a sequence number
    inline void setLastChunk(bool isLastChunk) noexcept { controlBlock->setLastChunk(isLastChunk); }

    /// @brief retrieves if this is the last chunk
    [[nodiscard]] constexpr bool isLastChunk() const noexcept { return controlBlock->isLastChunk(); }

    /// @brief set the creation timestamp in milliseconds
    inline void setCreationTimestampInMS(uint64_t value) noexcept { controlBlock->setCreationTimestamp(value); }

    ///@brief get the buffer's origin id (the operator id that creates this buffer).
    [[nodiscard]] constexpr OriginId getOriginId() const noexcept { return controlBlock->getOriginId(); }

    ///@brief get the buffer's statistic id (where it was last touched).
    [[nodiscard]] constexpr StatisticId getStatisticId() const noexcept { return controlBlock->getStatisticId(); }

    ///@brief set the buffer's origin id (the operator id that creates this buffer).
    inline void setOriginId(OriginId id) noexcept { controlBlock->setOriginId(id); }

    ///@brief set the buffer's statistic id (where it was last touched).
    inline void setStatisticId(StatisticId statisticId) noexcept { controlBlock->setStatisticId(statisticId); }

    ///@brief attach a child tuple buffer to the parent. the child tuple buffer is then identified via NestedTupleBufferKey
    [[nodiscard]] NestedTupleBufferKey storeChildBuffer(TupleBuffer& buffer) const noexcept;

    ///@brief retrieve a child tuple buffer via its NestedTupleBufferKey
    [[nodiscard]] TupleBuffer loadChildBuffer(NestedTupleBufferKey bufferIndex) const noexcept;

    [[nodiscard]] constexpr uint32_t getNumberOfChildrenBuffer() const noexcept {
        return controlBlock->getNumberOfChildrenBuffer();
    }

    /**
     * @brief returns true if there is enought space to write
     * @param used space
     * @param needed space
     * @return TupleBuffer
     */
    bool hasSpaceLeft(uint64_t used, uint64_t needed) const;

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

/**
 * @brief Allocates an object of T in the tuple buffer.
 * Set the number of tuples to one.
 * @tparam T
 * @param buffer
 * @return T+
 */
template<typename T>
T* allocateWithin(TupleBuffer& buffer) {
    auto ptr = new (buffer.getBuffer()) T();
    buffer.setNumberOfTuples(1);
    return ptr;
};

}// namespace NES::Runtime
#endif// NES_RUNTIME_INCLUDE_RUNTIME_TUPLEBUFFER_HPP_
