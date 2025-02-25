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

#ifndef NES_COMMON_INCLUDE_SEQUENCING_NONBLOCKINGMONOTONICSEQQUEUE_HPP_
#define NES_COMMON_INCLUDE_SEQUENCING_NONBLOCKINGMONOTONICSEQQUEUE_HPP_

#include <Exceptions/RuntimeException.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <list>
#include <memory>

namespace NES::Sequencing {

template<class T>
using ContainerState = std::tuple<SequenceNumber, ChunkNumber, uint64_t, T>;

/**
 * @brief This class implements a non blocking monotonic sequence queue,
 * which is mainly used as a foundation for an efficient watermark processor.
 * This queue receives values of type T with an associated sequence number and
 * returns the value with the highest sequence number in strictly monotonic increasing order.
 *
 * Internally this queue is implemented as a linked-list of blocks and each block stores a array of <seq,T> pairs.
 * |------- |       |------- |
 * | s1, s2 | ----> | s3, s4 |
 * |------- |       | ------ |
 *
 * The high level flow is the following:
 * If we receive the following sequence of input pairs <seq,T>:
 * Note that the events with seq 3 and 5 are received out-of-order.
 *
 * [<1,T1>,<2,T2>,<4,T4>,<6,T6>,<3,T3>,<7,T7>,<5,T5>]
 *
 * GetCurrentValue will return the following sequence of current values:
 * [<1,T1>,<2,T2>,<2,T2>,<2,T2>,<2,T2>,<4,T6>,<7,T7>]
 *
 *
 * @tparam T
 * @tparam blockSize
 */
template<class T, uint64_t blockSize = 10000>
class NonBlockingMonotonicSeqQueue {
  private:
    /**
     * @brief This class contains the state for one sequence number. It checks if the SeqQueue has seen all chunks for
     * the sequence number.
     */
    class Container {
      public:
        Container()
            : seqNumber(INVALID_SEQ_NUMBER), lastChunkNumber(INVALID_CHUNK_NUMBER), seenChunks(0),
              mergeValues(Util::updateAtomicMax<T>) {}

        /**
         * @brief This methods emplaces <chunkNumber, lastChunk, value> into the container.
         * @param chunkNumber
         * @param lastChunk
         * @param value
         */
        void emplaceChunk(ChunkNumber chunkNumber, bool lastChunk, T value) {
            if (lastChunk) {
                this->lastChunkNumber = chunkNumber;
            }
            ++seenChunks;
            mergeValues(this->value, value);
        }

        void setSeqNumber(const SequenceNumber& seqNumber) { this->seqNumber = seqNumber; }

        /**
         * @brief Returns the value
         * @return T
         */
        T getValue() const { return value; }

        /**
         * @brief Gets the sequence number corresponding to this container
         * @return SequenceNumber
         */
        [[nodiscard]] SequenceNumber getSeqNumber() const { return seqNumber; }

        bool seenAllChunks() { return (lastChunkNumber != INVALID_CHUNK_NUMBER) && (seenChunks == lastChunkNumber); }

        /**
         * @brief Gets the state of every container with valid sequence number
         * @return container state
         */
        ContainerState<T> serialize() { return ContainerState<T>(seqNumber, lastChunkNumber, seenChunks, value); }

        /**
         * @brief Gets the state of every container with valid sequence number
         * @return container state
         */
        void deserialize(ContainerState<T> state) {
            seqNumber = std::get<0>(state);
            lastChunkNumber = std::get<1>(state);
            seenChunks = std::get<2>(state);
            value = std::get<3>(state);
        }

      private:
        SequenceNumber seqNumber;
        ChunkNumber lastChunkNumber;
        std::atomic<uint64_t> seenChunks;
        std::atomic<T> value;
        std::function<void(std::atomic<T>&, const T&)> mergeValues;
    };

    /**
     * @brief Block of values, which is one element in the linked-list.
     * If the next block exists *next* contains the reference.
     */
    class Block {
      public:
        explicit Block(uint64_t blockIndex) : blockIndex(blockIndex){};
        ~Block() = default;
        const uint64_t blockIndex;
        std::array<Container, blockSize> log;
        std::shared_ptr<Block> next = std::shared_ptr<Block>();
    };

  public:
    NonBlockingMonotonicSeqQueue() : head(std::make_shared<Block>(0)), currentSeq(0) {}

    ~NonBlockingMonotonicSeqQueue() = default;

    NonBlockingMonotonicSeqQueue& operator=(const NonBlockingMonotonicSeqQueue& other) {
        head = other.head;
        currentSeq = other.currentSeq.load();
        return *this;
    }

    /**
     * @brief Emplace a new element to the queue.
     * This method can be called concurrently.
     * However, only one element with a given sequence number can be inserted.
     * @param sequenceData of the new element.
     * @param value of the new value.
     * @throws RuntimeException if an element with the same sequence number was already inserted.
     */
    void emplace(SequenceData sequenceData, T value) {
        if (sequenceData.sequenceNumber < currentSeq) {
            // TODO reactivate fatal error
//            NES_FATAL_ERROR("Invalid sequence number {} as it is < {}", sequenceData.sequenceNumber, currentSeq);
            // TODO add exception, currently tests fail
            // throw Exceptions::RuntimeException("Invalid sequence number " + std::to_string(sequenceNumber)
            //                                   + " as it is <= " + std::to_string(currentSeq));
        }
        // First emplace the value to the specific block of the sequenceNumber.
        // After this call it is safe to assume that a block, which contains the sequenceNumber exists.
        emplaceValueInBlock(sequenceData, value);
        // Try to shift the current sequence number
        shiftCurrentValue();
    }

    /**
     * @brief Returns the current value.
     * This method is thread save, however it is not guaranteed that current value dose not change concurrently.
     * @return T value
     */
    auto getCurrentValue() const {
        auto currentBlock = std::atomic_load(&head);
        // get the current sequence number and access the associated block
        auto currentSequenceNumber = currentSeq.load();
        auto targetBlockIndex = currentSequenceNumber / blockSize;
        currentBlock = getTargetBlock(currentBlock, targetBlockIndex);
        // read the value from the correct slot.
        auto seqIndexInBlock = currentSequenceNumber % blockSize;
        auto& value = currentBlock->log[seqIndexInBlock];
        return value.getValue();
    }

    /**
     * @warning !NOTE!
     * Do not run this function concurrently
     * @brief serialize watermark processor
     * @return vector of ContainerState
     */
    std::vector<ContainerState<T>> serialize() const {
        std::vector<ContainerState<T>> states;
        auto currentBlock = head;
        // go over all blocks and serialize them
        while (currentBlock != nullptr) {
            for (auto& container : currentBlock->log) {
                if (container.getValue() != INVALID_SEQ_NUMBER && container.getSeqNumber() >= currentSeq) {
                    states.emplace_back(container.serialize());
                }
            }
            currentBlock = currentBlock->next;
        }

        return states;
    }

    /**
     * @warning !NOTE!
     * Do not run this function concurrently
     * @brief Gets the state of every container with valid sequence number
     * @return container state
     */
    void deserialize(std::vector<ContainerState<T>> states) {
        if (states.empty()) {
            // nothing to recreate
            return;
        }

        // get new current sequence number
        auto firstSeqNumber = std::get<0>(states[0]);
        // set read sequence number
        currentSeq = firstSeqNumber;
        head = std::make_shared<Block>(firstSeqNumber / blockSize);
        auto currBlock = head;

        // go over states and set them to the right block
        for (auto& state : states) {
            auto targetSeqNumber = std::get<0>(state);
            auto targetBlockIndex = targetSeqNumber / blockSize;
            while (currBlock->blockIndex < targetBlockIndex) {
                auto nextBlock = currBlock->next;
                if (nextBlock == nullptr) {
                    // create this blocks if they don't exist
                    auto newBlock = std::make_shared<Block>(currBlock->blockIndex + 1);
                    currBlock->next = newBlock;
                    currBlock = newBlock;
                } else {
                    currBlock = currBlock->next;
                }
            }
            currBlock->log[targetSeqNumber % blockSize].deserialize(state);
        }
    }

  private:
    /**
     * @brief Emplace a value T to the specific location of the passed sequence number
     *
     * The method is split in two phased:
     * 1. Find the correct block for this sequence number. If the block not yet exists we add a new block to the linked-list.
     * 2. Place value at the correct slot associated with the sequence number.
     *
     * @param seq the sequence number of the value
     * @param value the value that should be stored.
     */
    void emplaceValueInBlock(SequenceData seq, T value) {
        // Each block contains blockSize elements and covers sequence numbers from
        // [blockIndex * blockSize] till [blockIndex * blockSize + blockSize]
        // Calculate the target block index, which contains the sequence number
        auto targetBlockIndex = seq.sequenceNumber / blockSize;
        // Lookup the current block
        auto currentBlock = std::atomic_load(&head);
        // if the blockIndex is smaller the target block index we travers the next block
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (nextBlock == nullptr) {
                auto newBlock = std::make_shared<Block>(currentBlock->blockIndex + 1);
                std::atomic_compare_exchange_weak(&currentBlock->next, &nextBlock, newBlock);
                // we don't care if this or another thread succeeds, as we just start over again in the loop
                // and use what ever is now stored in currentBlock.next
            } else {
                // move to the next block
                currentBlock = nextBlock;
            }
        }

        auto start = currentBlock->blockIndex * blockSize;
        auto end = currentBlock->blockIndex * blockSize + blockSize;
        // check if we really found the correct block
        if (!(seq.sequenceNumber >= start
              && seq.sequenceNumber < end)) {
            NES_ERROR("Wrong block for seq {}, expected indext between {} and {}", seq.sequenceNumber, start, end);
            throw Exceptions::RuntimeException("The found block is wrong");
        }

        // Emplace value in block
        auto seqIndexInBlock = seq.sequenceNumber % blockSize;
        currentBlock->log[seqIndexInBlock].setSeqNumber(seq.sequenceNumber);
        currentBlock->log[seqIndexInBlock].emplaceChunk(seq.chunkNumber, seq.lastChunk, value);
    }

    /**
     * @brief This method shifts tries to shift the current value.
     * To this end, it checks if the next expected sequence number (currentSeq + 1) is already inserted.
     * If the next sequence number is available it replaces the currentSeq with the next one.
     * If the next sequence number is in a new block this method also replaces the pointer to the next block.
     */
    void shiftCurrentValue() {
        auto checkForUpdate = true;
        while (checkForUpdate) {
            auto currentBlock = std::atomic_load(&head);
            // we are looking for the next sequence number
            auto currentSequenceNumber = currentSeq.load();
            // find the correct block, that contains the current sequence number.
            auto targetBlockIndex = currentSequenceNumber / blockSize;
            currentBlock = getTargetBlock(currentBlock, targetBlockIndex);

            // check if next value is set
            // next seqNumber
            auto nextSeqNumber = currentSequenceNumber + 1;
            if (nextSeqNumber % blockSize == 0) {
                // the next sequence number is the first element in the next block.
                auto nextBlock = std::atomic_load(&currentBlock->next);
                if (nextBlock != nullptr) {
                    // this will always be the first element
                    auto& value = nextBlock->log[0];
                    if (value.getSeqNumber() == nextSeqNumber && value.seenAllChunks()) {
                        // Modify currentSeq and head
                        if (std::atomic_compare_exchange_weak(&currentSeq, &currentSequenceNumber, nextSeqNumber)) {
                            std::atomic_compare_exchange_weak(&head, &currentBlock, nextBlock);
                        }
                        continue;
                    }
                }
            } else {
                auto seqIndexInBlock = nextSeqNumber % blockSize;
                auto& value = currentBlock->log[seqIndexInBlock];
                if (value.getSeqNumber() == nextSeqNumber && value.seenAllChunks()) {
                    // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                    std::atomic_compare_exchange_weak(&currentSeq, &currentSequenceNumber, nextSeqNumber);
                    continue;
                }
            }
            checkForUpdate = false;
        }
    }

    /**
     * @brief This function traverses the linked list of blocks, till the target block index is found.
     * It assumes, that the target block index exists. If not, the function throws a runtime exception.
     * @param currentBlock the start block, usually the head.
     * @param targetBlockIndex the target address
     * @return the found block, which contains the target block index.
     */
    std::shared_ptr<Block> getTargetBlock(std::shared_ptr<Block> currentBlock, uint64_t targetBlockIndex) const {
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (!nextBlock) {
                throw Exceptions::RuntimeException("The next block dose not exist. This should not happen here.");
            }
            // move to the next block
            currentBlock = nextBlock;
        }
        return currentBlock;
    }

    // Stores a reference to the current block
    std::shared_ptr<Block> head;
    // Stores the current sequence number
    std::atomic<SequenceNumber> currentSeq;
};

}// namespace NES::Sequencing

#endif// NES_COMMON_INCLUDE_SEQUENCING_NONBLOCKINGMONOTONICSEQQUEUE_HPP_
