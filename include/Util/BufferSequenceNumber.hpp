/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_BUFFERSEQUENCENUMBER_HPP
#define NES_BUFFERSEQUENCENUMBER_HPP

#include <cstdint>
#include <functional>
#include <memory>

namespace NES {

/**
 * @brief The Buffer Sequence Number class encapsulates a unique id for every tuple buffer in the system.
 * It consists out of a sequence number and an origin id. Their combination allows uniquely define a tuple buffer in the system.
 */
class BufferSequenceNumber {

  public:
    /**
     * @brief Default constructor, which creates a buffer sequence number
     */
    BufferSequenceNumber() = default;
    /**
     * @brief Constructor, which creates new buffer sequence number out of pair sequnce number and origin id
     * @param sequenceNumber sequence number
     * @param originId origin id
     * @return buffer sequence number
     */
    BufferSequenceNumber(uint64_t sequenceNumber, uint64_t originId) : sequenceNumber(sequenceNumber), originId(originId){};

    /**
     * @brief Getter for a sequence number of a buffer sequence number
     * @return sequence number
     */
    uint64_t getSequenceNumber() const { return sequenceNumber; }

    /**
     * @brief Getter for an origin id of a buffer sequence number
     * @return origin id
     */
    uint64_t getOriginId() const { return originId; }

    /**
     * @brief Methods to check if a buffer sequence number is a valid pair
     * @return true if this sequence number is valid
     */
    bool isValid() { return sequenceNumber > 0 && originId > 0; }

  private:
    int sequenceNumber;
    int originId;
    friend bool operator<(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber < rhs.sequenceNumber;
    }
    friend bool operator<=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber <= rhs.sequenceNumber;
    }
    friend bool operator>(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber > rhs.sequenceNumber;
    }
    friend bool operator>=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber >= rhs.sequenceNumber;
    }
    friend bool operator==(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber == rhs.sequenceNumber;
    }
    friend bool operator!=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs) {
        return lhs.sequenceNumber != rhs.sequenceNumber;
    }
};

using BufferSequenceNumberPtr = std::shared_ptr<BufferSequenceNumber>;
}// namespace NES

namespace std {
template<>
struct hash<NES::BufferSequenceNumber> {
    uint64_t operator()(const NES::BufferSequenceNumber& sn) const {
        typedef unsigned long ulong;
        return ulong(sn.getOriginId() << 16) | ulong(sn.getSequenceNumber() << 8);
    }
};
}// namespace std
#endif//NES_BUFFERSEQUENCENUMBER_HPP
