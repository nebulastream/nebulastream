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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_NONBLOCKINGWATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_NONBLOCKINGWATERMARKPROCESSOR_HPP_

#include <Util/Logger/Logger.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include <algorithm>
#include <assert.h>
#include <atomic>
#include <memory>

namespace NES::Experimental {

template<class T, uint64_t blockSize>
class NonBlockingMonotonicSeqQueue {

    class Block {
      public:
        Block(uint64_t blockIndex) : blockIndex(blockIndex){};
        const uint64_t blockIndex;
        std::array<std::tuple<uint64_t, T>, blockSize> log;
        std::shared_ptr<Block> next;
    };

    class Node {
      public:
        Node(uint64_t seq) : seq(seq){};
        const uint64_t seq;
        const T value;
        std::shared_ptr<Node> next;
    };

  public:
    NonBlockingMonotonicSeqQueue() { head = new Node(); }

    void emplaceValueInBlock(uint64_t seq, T value) {
        // The block index, which contains the sequence number.
        auto targetBlockIndex = seq % blockSize;
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
                atomic_compare_exchange_weak(nextBlock, &currentBlock->next, newBlock);
                // we don't if this or another thread succeeds, as we just start over again in the loop
                // and use what ever is now stored in currentBlock.next.
                continue;
            }
            // move to the next block
            currentBlock = nextBlock;
        }

        // check if we really found the correct block
        if (!(currentBlock->blockIndex * blockSize >= seq && currentBlock->blockIndex * blockSize + blockSize < seq)) {
            NES_THROW_RUNTIME_ERROR("The found block is wrong, we expected ");
        }

        // Emplace value in block
        // It is safe to perform this operation without atomics as no other thread will have the same sequence number,
        // and thus can modify this value.
        auto seqIndexInBlock = seq - (currentBlock->blockIndex * blockSize);
        currentBlock->log[seqIndexInBlock] = std::make_tuple(seq, value);
    }

    uint64_t shiftCurrent() {
        auto checkForUpdate = true;
        while (checkForUpdate) {
            auto currentBlock = std::atomic_load(&head);
            // we are looking for the next sequence number
            auto seqNumber = currentSeq.load();
            while (currentBlock->blockIndex < seqNumber) {
                // append new block if the next block is a nullptr
                auto nextBlock = std::atomic_load(&currentBlock->next);
                if (nextBlock == nullptr) {
                    NES_THROW_RUNTIME_ERROR("The next block dose not exists. This should not happen here.");
                }
                // move to the next block
                currentBlock = nextBlock;
            }

            // check if next value is set
            // next seqNumber
            auto nextSeqNumber = seqNumber + 1;
            if (nextSeqNumber % blockSize == 0) {
                // the next sequence number is the first element in the next block.
                auto nextBlock = std::atomic_load(&currentBlock->next);
                if (nextBlock != nullptr) {
                    // this will always be the first element
                    auto& value = nextBlock->log[0];
                    if (std::get<0>(value) == nextSeqNumber) {
                        // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                        if (std::atomic_compare_exchange_strong(&currentSeq, &nextSeqNumber, nextSeqNumber)) {
                            std::atomic_compare_exchange_strong(&head, &nextBlock, nextBlock);
                        };
                        continue;
                    }
                }
            } else {
                auto seqIndexInBlock = nextSeqNumber - (currentBlock->blockIndex * blockSize);
                auto& value = currentBlock->log[seqIndexInBlock];
                if (std::get<0>(value) == nextSeqNumber) {
                    // the next sequence number is still in the current block thus we only have to exchange the currentSeq.
                    std::atomic_compare_exchange_strong(&currentSeq, &nextSeqNumber, nextSeqNumber);
                    continue;
                }
            }
            checkForUpdate = false;
        }
    }

    uint64_t update(uint64_t seq, T value) {

        // First we emplace the value to its specific block.
        // After this call it is safe to assume that a block, which contains seq exists.
        emplaceValueInBlock(seq, value);

        // We now try to shift the current sequence number
        shiftCurrent();
    }

  private:
    std::shared_ptr<Node> head;
    std::atomic<uint64_t> currentSeq;
};

}// namespace NES::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_NONBLOCKINGWATERMARKPROCESSOR_HPP_