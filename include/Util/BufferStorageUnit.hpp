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

#ifndef NES_BUFFERSTORAGEUNIT_H
#define NES_BUFFERSTORAGEUNIT_H

class BufferStorageUnit {
  public:
    BufferStorageUnit(const NES::BufferSequenceNumber& sequenceNumber, const NES::Runtime::TupleBuffer& tupleBuffer)
        : sequenceNumber(sequenceNumber), tupleBuffer(tupleBuffer) {};
    const NES::BufferSequenceNumber& getSequenceNumber() const { return sequenceNumber; }
    const NES::Runtime::TupleBuffer& getTupleBuffer() const { return tupleBuffer; }

  private:
    NES::BufferSequenceNumber sequenceNumber;
    NES::Runtime::TupleBuffer tupleBuffer;
    friend bool operator<(const BufferStorageUnit& lhs, const BufferStorageUnit& rhs) { return lhs.sequenceNumber < rhs.sequenceNumber; }
    friend bool operator>(const BufferStorageUnit& lhs, const BufferStorageUnit& rhs) { return lhs.sequenceNumber > rhs.sequenceNumber; }
};

#endif//NES_BUFFERSTORAGEUNIT_H
