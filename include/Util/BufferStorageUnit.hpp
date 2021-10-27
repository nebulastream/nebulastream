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

#ifndef NES_INCLUDE_UTIL_BUFFER_STORAGE_UNIT_HPP_
#define NES_INCLUDE_UTIL_BUFFER_STORAGE_UNIT_HPP_

namespace NES::Runtime {
/**
 * @brief The Buffer Storage Unit class encapsulates a pair<tuple id, pointer to the tuple>
 */
class BufferStorageUnit {
  public:
    /**
     * @brief Constructor, which creates new buffer storage unit out of pair buffer sequnce number and a tiple buffer pointer
     * @param sequenceNumber pair <sequence number, origin id>
     * @param tupleBuffer pointer to the tuple buffer with a given sequence number
     * @return buffer storage unit
     */
    BufferStorageUnit(const NES::BufferSequenceNumber& sequenceNumber, const NES::Runtime::TupleBuffer& tupleBuffer)
        : sequenceNumber(sequenceNumber), tupleBuffer(tupleBuffer){};

    /**
     * @brief Getter for a sequence number of a buffer storage unit
     * @return sequence number
     */
    const NES::BufferSequenceNumber& getSequenceNumber() const { return sequenceNumber; }

    /**
     * @brief Getter for a tuple buffer pointer
     * @return tuple buffer pointer
     */
    const NES::Runtime::TupleBuffer& getTupleBuffer() const { return tupleBuffer; }

  private:
    NES::BufferSequenceNumber sequenceNumber;
    NES::Runtime::TupleBuffer tupleBuffer;
    friend bool operator<(const std::shared_ptr<BufferStorageUnit>& lhs, const std::shared_ptr<BufferStorageUnit>& rhs) {
        return lhs->sequenceNumber < rhs->sequenceNumber;
    }
    friend bool operator>(const std::shared_ptr<BufferStorageUnit>& lhs, const std::shared_ptr<BufferStorageUnit>& rhs) {
        return lhs->sequenceNumber > rhs->sequenceNumber;
    }
};

using BufferStorageUnitPtr = std::shared_ptr<BufferStorageUnit>;
}// namespace NES::Runtime
#endif  // NES_INCLUDE_UTIL_BUFFER_STORAGE_UNIT_HPP_
