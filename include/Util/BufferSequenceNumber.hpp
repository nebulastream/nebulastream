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
namespace NES {

/**
 * @brief The Buffer Sequence Number class encapsulates a unique id for every tuple buffer in the system.
 * It consists out of a sequence number and an origin id. Their combination allows uniquely define a tuple buffer in the system.
 */
class BufferSequenceNumber {
    int64_t sequenceNumber;
    int64_t originId;

  public:
    BufferSequenceNumber(int64_t sequenceNumber, int64_t originId) : sequenceNumber(sequenceNumber), originId(originId) {}

  private:
    friend bool operator<(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber < rightSN.sequenceNumber && leftSN.originId < rightSN.originId; }
    friend bool operator<=(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber <= rightSN.sequenceNumber && leftSN.originId <= rightSN.originId; }
    friend bool operator>(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber > rightSN.sequenceNumber && leftSN.originId > rightSN.originId; }
    friend bool operator>=(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber >= rightSN.sequenceNumber && leftSN.originId >= rightSN.originId; }
    friend bool operator==(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber == rightSN.sequenceNumber && leftSN.originId == rightSN.originId; }
    friend bool operator!=(const BufferSequenceNumber& leftSN, const BufferSequenceNumber& rightSN) { return leftSN.sequenceNumber != rightSN.sequenceNumber && leftSN.originId != rightSN.originId; }
};


} // namespace NES
#endif//NES_BUFFERSEQUENCENUMBER_HPP
