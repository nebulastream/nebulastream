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

#include <cstdint>
#include <functional>
#include <memory>

namespace NES
{

/**
 * @brief The Buffer Sequence Number class encapsulates a unique id for every tuple buffer in the system.
 * It consists out of a sequence number and an origin id. Their combination allows uniquely define a tuple buffer in the system.
 */
class BufferSequenceNumber
{
public:
    BufferSequenceNumber() = default;
    /**
     * @brief Constructor, which creates new buffer sequence number out of pair sequnce number and origin id
     * @param sequenceNumber sequence number
     * @param originId origin id
     * @return buffer sequence number
     */
    BufferSequenceNumber(uint64_t sequenceNumber, uint64_t originId) : sequenceNumber(sequenceNumber), originId(originId) { };

    /**
     * @brief Getter for a sequence number of a buffer sequence number
     * @return sequence number
     */
    uint64_t getSequenceNumber() const;

    /**
     * @brief Getter for an origin id of a buffer sequence number
     * @return origin id
     */
    uint64_t getOriginId() const;

private:
    uint64_t sequenceNumber;
    uint64_t originId;

    friend bool operator<(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber < rhs.sequenceNumber;
    }

    friend bool operator<=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber <= rhs.sequenceNumber;
    }

    friend bool operator>(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber > rhs.sequenceNumber;
    }

    friend bool operator>=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber >= rhs.sequenceNumber;
    }

    friend bool operator==(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber == rhs.sequenceNumber;
    }

    friend bool operator!=(const BufferSequenceNumber& lhs, const BufferSequenceNumber& rhs)
    {
        return lhs.sequenceNumber != rhs.sequenceNumber;
    }
};

}

namespace std
{
template <>
struct hash<NES::BufferSequenceNumber>
{
    uint64_t operator()(const NES::BufferSequenceNumber& sn) const { return sn.getSequenceNumber() ^ sn.getOriginId(); }
};
}
