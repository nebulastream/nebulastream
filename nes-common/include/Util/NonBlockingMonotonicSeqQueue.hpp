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

#ifndef NES_INCLUDE_UTIL_NONBLOCKINGWATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_UTIL_NONBLOCKINGWATERMARKPROCESSOR_HPP_

#include <Util/Logger/Logger.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <memory>

namespace NES::Util {

/**
 * @brief This class implements a non blocking monotonic sequence queue.
 * This queue receives values of type T with an associated sequence number and
 * returns the value with the highest sequence number in strictly monotonic increasing order.
 *
 * Internally this queue is implemented as a linked-list of blocks and each block stores a list of <seq,T> pairs.
 * |------- |       |------- |
 * | s1, s2 | ----> | s3, s4 |
 * |------- |       | ------ |
 *
 * @tparam T
 * @tparam blockSize
 */
template<class T, uint64_t blockSize = 100>
class NonBlockingMonotonicSeqQueue {
  private:
    /**
     * @brief Container, which contains the sequence number and the value.
     */
    struct Container {
        uint64_t seq;
        T value;
    };

    /**
     * @brief Block of values, which is one element in the linked-list.
     * If the next block exists *next* contains the reference.
     */
    class Block {
      public:
        Block(uint64_t blockIndex) : blockIndex(blockIndex){};
        Block(const Block& other) : blockIndex(other.blockIndex){};
        ~Block() = default;
        const uint64_t blockIndex;
        std::array<Container, blockSize> log = {};
        std::shared_ptr<Block> next = std::shared_ptr<Block>();
    };

  public:
    NonBlockingMonotonicSeqQueue() : head(std::make_shared<Block>(0)), currentSeq(0) {}
    ~NonBlockingMonotonicSeqQueue() {}

    /**
     * @brief Emplace the next element to the queue and
     * @param seq
     * @param value
     */
    void emplace(uint64_t seq, T value) {
        // First we emplace the value to its specific block.
        // After this call it is safe to assume that a block, which contains seq exists.
        emplaceValueInBlock(seq, value);
        // We now try to shift the current sequence number
        shiftCurrent();
    }

    /**
     * @brief Returns the current value.
     * @return T
     */
    auto getCurrentValue() {
        auto currentBlock = std::atomic_load(&head);
        // we are looking for the next sequence number
        auto currentSequenceNumber = currentSeq.load();
        auto targetBlockIndex = currentSequenceNumber / blockSize;
        currentBlock = getTargetBlock(currentBlock, targetBlockIndex);
        auto seqIndexInBlock = currentSequenceNumber - (currentBlock->blockIndex * blockSize);
        auto& value = currentBlock->log[seqIndexInBlock];
        return value.value;
    }

  private:
    void emplaceValueInBlock(uint64_t seq, T value) {
        // The block index, which contains the sequence number.
        auto targetBlockIndex = seq / blockSize;
        // Find the right block
        auto currentBlock = std::atomic_load(&head);
        // Each block contains blockSize elements and covers sequence numbers from
        // [blockIndex * blockSize] till [blockIndex * blockSize + blockSize]
        // if the end seq number is smaller than the searched seq we have to go to the next block.
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (nextBlock == nullptr) {
                auto newBlock = std::make_shared<Block>(currentBlock->blockIndex + 1);
                std::atomic_compare_exchange_strong(&currentBlock->next, &nextBlock, newBlock);
                // we don't if this or another thread succeeds, as we just start over again in the loop
                // and use what ever is now stored in currentBlock.next.
            } else {
                // move to the next block
                currentBlock = nextBlock;
            }
        }

        // check if we really found the correct block
        if (!(seq >= currentBlock->blockIndex * blockSize && seq < currentBlock->blockIndex * blockSize + blockSize)) {
            NES_THROW_RUNTIME_ERROR("The found block is wrong, we expected ");
        }

        // Emplace value in block
        // It is safe to perform this operation without atomics as no other thread will have the same sequence number,
        // and thus can modify this value.
        auto seqIndexInBlock = seq - (currentBlock->blockIndex * blockSize);
        currentBlock->log[seqIndexInBlock].seq = seq;
        currentBlock->log[seqIndexInBlock].value = value;
    }

    void shiftCurrent() {
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
                    if (value.seq == nextSeqNumber) {
                        // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                        if (std::atomic_compare_exchange_strong(&currentSeq, &currentSequenceNumber, nextSeqNumber)) {
                            NES_DEBUG("Swap HEAD: remove " << currentBlock->blockIndex << " - " << currentBlock.use_count());
                            std::atomic_compare_exchange_strong(&head, &currentBlock, nextBlock);
                        };
                        continue;
                    }
                }
            } else {
                auto seqIndexInBlock = nextSeqNumber - (currentBlock->blockIndex * blockSize);
                auto& value = currentBlock->log[seqIndexInBlock];
                if (value.seq == nextSeqNumber) {
                    // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                    std::atomic_compare_exchange_strong(&currentSeq, &currentSequenceNumber, nextSeqNumber);
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
    std::shared_ptr<Block> getTargetBlock(std::shared_ptr<Block> currentBlock, uint64_t targetBlockIndex) {
        while (currentBlock->blockIndex < targetBlockIndex) {
            // append new block if the next block is a nullptr
            auto nextBlock = std::atomic_load(&currentBlock->next);
            if (!nextBlock) {
                NES_THROW_RUNTIME_ERROR("The next block dose not exists. This should not happen here.");
            }
            // move to the next block
            currentBlock = nextBlock;
        }
        return currentBlock;
    }

  private:
    std::shared_ptr<Block> head;
    std::atomic<uint64_t> currentSeq;
};

}// namespace NES::Util

#endif//NES_INCLUDE_UTIL_NONBLOCKINGWATERMARKPROCESSOR_HPP_