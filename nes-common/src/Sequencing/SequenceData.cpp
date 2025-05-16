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

#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>

namespace NES
{
SequenceData::SequenceData(SequenceNumber sequenceNumber, ChunkNumber chunkNumber, bool lastChunk)
    : sequenceNumber(sequenceNumber.getRawValue()), chunkNumber(chunkNumber.getRawValue()), lastChunk(lastChunk)
{
}

SequenceData::SequenceData()
    : sequenceNumber(INVALID_SEQ_NUMBER.getRawValue()), chunkNumber(INVALID_CHUNK_NUMBER.getRawValue()), lastChunk(false) { };

std::ostream& operator<<(std::ostream& os, const SequenceData& obj)
{
    os << "{SeqNumber: " << obj.sequenceNumber << ", ChunkNumber: " << obj.chunkNumber << ", LastChunk: " << obj.lastChunk << "}";
    return os;
}

}
