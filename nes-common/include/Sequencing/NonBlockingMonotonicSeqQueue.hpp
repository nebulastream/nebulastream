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

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/ChunkCollector.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sequencing
{

/// @brief This class implements a non blocking monotonic sequence queue,
/// which is mainly used as a foundation for an efficient watermark processor.
/// This queue receives values of type T with an associated sequence number and
/// returns the value with the highest sequence number in strictly monotonic increasing order.
///
/// Internally this queue is implemented as a linked-list of blocks and each block stores a array of <seq,T> pairs.
/// |------- |       |------- |
/// | s1, s2 | ----> | s3, s4 |
/// |------- |       | ------ |
///
/// The high level flow is the following:
/// If we receive the following sequence of input pairs <seq,T>:
/// Note that the events with seq 3 and 5 are received out-of-order.
///
/// [<1,T1>,<2,T2>,<4,T4>,<6,T6>,<3,T3>,<7,T7>,<5,T5>]
///
/// GetCurrentValue will return the following sequence of current values:
/// [<1,T1>,<2,T2>,<2,T2>,<2,T2>,<2,T2>,<4,T6>,<7,T7>]
template <class T, uint64_t BlockSize = 8192>
class NonBlockingMonotonicSeqQueue
{
private:
    /// @brief Container, which contains the sequence number and the value.
    struct Container
    {
        std::atomic<SequenceNumber::Underlying> seq;
        std::atomic<T> value;
    };

    /// @brief Block of values, which is one element in the linked-list.
    /// If the next block exists *next* contains the reference.
    class Block
    {
    public:
        explicit Block(size_t blockIndex) : blockIndex(blockIndex) { };
        ~Block() = default;
        const size_t blockIndex;
        std::array<Container, BlockSize> log = {};
        std::shared_ptr<Block> next = std::shared_ptr<Block>();
    };

public:
    NonBlockingMonotonicSeqQueue() : head(std::make_shared<Block>(0)), currentSeq(0) { }

    ~NonBlockingMonotonicSeqQueue() = default;

    NonBlockingMonotonicSeqQueue& operator=(const NonBlockingMonotonicSeqQueue& other)
    {
        head = other.head;
        currentSeq = other.currentSeq.load();
        return *this;
    }

    /// Stores `newValue` for every sequence number of the range of `sequenceData` once all of its chunks arrived.
    /// Ranges must not overlap, and all chunks of a range must carry the same offset.
    void emplace(SequenceData sequenceData, T newValue)
    {
        if (auto opt = chunks.collect(sequenceData, Timestamp(newValue)))
        {
            auto [last, value] = *opt;
            PRECONDITION(
                last.getRawValue() >= SequenceNumber::INITIAL + sequenceData.sequenceRangeOffset,
                "Sequence range [{} - {}, {}] starts below the first sequence number",
                last,
                sequenceData.sequenceRangeOffset,
                last);
            const auto first = last.getRawValue() - sequenceData.sequenceRangeOffset;
            INVARIANT(
                first > currentSeq.load(),
                "Invalid sequenceNumber: {} has already been seen. Current Sequence: {}",
                first,
                currentSeq.load());
            emplaceValues(first, last.getRawValue(), value.getRawValue());
            /// Pairs with the seq_cst loads in `lastInsertedFrom`: of this thread and a concurrently shifting one, at least one sees
            /// the other's write.
            std::atomic_thread_fence(std::memory_order::seq_cst);
            /// Try to shift the current sequence number
            shiftCurrentValue();
            chunks.releaseUpTo(SequenceNumber(currentSeq.load()));
        }
    }

    /// @brief Returns the current value.
    /// This method is thread save, however it is not guaranteed that current value dose not change concurrently.
    /// @return T value
    auto getCurrentValue() const
    {
        auto currentBlock = std::atomic_load(&head);
        /// get the current sequence number and access the associated block
        auto currentSequenceNumber = currentSeq.load();
        auto targetBlockIndex = currentSequenceNumber / BlockSize;
        currentBlock = getTargetBlock(currentBlock, targetBlockIndex);
        /// read the value from the correct slot.
        auto seqIndexInBlock = currentSequenceNumber % BlockSize;
        auto& value = currentBlock->log[seqIndexInBlock];
        return value.value.load(std::memory_order_relaxed);
    }

    [[nodiscard]] size_t getNumberOfChunkNodes() const { return chunks.getNumberOfNodes(); }

private:
    /// Stores `value` at the slots of [`first`, `last`] and appends the blocks they need.
    void emplaceValues(const SequenceNumber::Underlying first, const SequenceNumber::Underlying last, const T value)
    {
        auto currentBlock = std::atomic_load(&head);
        for (auto seq = first; seq <= last; ++seq)
        {
            while (currentBlock->blockIndex < seq / BlockSize)
            {
                auto nextBlock = std::atomic_load(&currentBlock->next);
                if (nextBlock == nullptr)
                {
                    /// If another thread appended first, the loop continues with its block.
                    std::atomic_compare_exchange_strong(
                        &currentBlock->next, &nextBlock, std::make_shared<Block>(currentBlock->blockIndex + 1));
                }
                else
                {
                    currentBlock = std::move(nextBlock);
                }
            }
            INVARIANT(
                seq >= currentBlock->blockIndex * BlockSize,
                "sequence number: {} was in prior block: {}",
                seq,
                currentBlock->blockIndex * BlockSize);

            /// The value is stored first, so whoever sees the sequence number also sees the value.
            auto& slot = currentBlock->log[seq % BlockSize];
            slot.value.store(value, std::memory_order::relaxed);
#ifndef NO_ASSERT
            INVARIANT(slot.seq.exchange(seq, std::memory_order::release) != seq, "sequence number: {} was emplaced twice", seq);
#else
            slot.seq.store(seq, std::memory_order::release);
#endif
        }
    }

    /// Advances `currentSeq` to the end of the run of inserted sequence numbers that follows it, and `head` to its block.
    void shiftCurrentValue()
    {
        /// Loads `head` before `currentSeq`, so `head` is never past the block of `currentSeq`.
        auto currentBlock = std::atomic_load(&head);
        auto currentSequenceNumber = currentSeq.load();
        while (true)
        {
            currentBlock = getTargetBlock(std::move(currentBlock), currentSequenceNumber / BlockSize);
            const auto lastInserted = lastInsertedFrom(currentBlock.get(), currentSequenceNumber);
            if (lastInserted == currentSequenceNumber)
            {
                return;
            }
            if (currentSeq.compare_exchange_strong(currentSequenceNumber, lastInserted))
            {
                /// The thread whose exchange moves `currentSeq` into a new block advances `head`.
                const auto changesBlock = lastInserted / BlockSize != currentSequenceNumber / BlockSize;
                currentSequenceNumber = lastInserted;
                if (changesBlock)
                {
                    currentBlock = getTargetBlock(std::move(currentBlock), currentSequenceNumber / BlockSize);
                    advanceHead(currentBlock);
                }
            }
        }
    }

    /// Returns the end of the run of inserted sequence numbers that follows `from`, or `from` if the next one is missing.
    static SequenceNumber::Underlying lastInsertedFrom(const Block* block, const SequenceNumber::Underlying from)
    {
        auto last = from;
        while (true)
        {
            const auto next = last + 1;
            if (next % BlockSize == 0)
            {
                /// Stays valid, as the caller holds the first block and `next` is never reset.
                block = std::atomic_load(&block->next).get();
            }
            if (block == nullptr || block->log[next % BlockSize].seq.load(std::memory_order::seq_cst) != next)
            {
                return last;
            }
            last = next;
        }
    }

    void advanceHead(const std::shared_ptr<Block>& block)
    {
        auto expectedHead = std::atomic_load(&head);
        while (expectedHead->blockIndex < block->blockIndex && !std::atomic_compare_exchange_weak(&head, &expectedHead, block))
        {
        }
    }

    /// @brief This function traverses the linked list of blocks, till the target block index is found.
    /// It assumes, that the target block index exists. If not, the function throws an Invariant.
    /// @param currentBlock the start block, usually the head.
    /// @param targetBlockIndex the target address
    /// @return the found block, which contains the target block index.
    std::shared_ptr<Block> getTargetBlock(std::shared_ptr<Block> currentBlock, uint64_t targetBlockIndex) const
    {
        while (currentBlock->blockIndex < targetBlockIndex)
        {
            /// append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            PRECONDITION(nextBlock, "Number of blocks in queue is smaller than targetBlockIndex: {}", targetBlockIndex);
            /// move to the next block
            currentBlock = nextBlock;
        }
        return currentBlock;
    }

    /// Stores a reference to the current block
    std::shared_ptr<Block> head;
    /// Stores the current sequence number
    std::atomic<SequenceNumber::Underlying> currentSeq;
    ChunkCollector<BlockSize> chunks;
};

}
