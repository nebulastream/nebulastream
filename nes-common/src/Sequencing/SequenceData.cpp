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

[[nodiscard]] std::string SequenceData::toString() const
{
    std::ostringstream oss;
    oss << *this;
    return oss.str();
}

bool SequenceData::operator<=(const SequenceData& other) const
{
    return ((*this < other)) || ((*this) == other);
}

bool SequenceData::operator<(const SequenceData& other) const
{
    if (sequenceNumber == other.sequenceNumber)
    {
        if (chunkNumber == other.chunkNumber)
        {
            return lastChunk != other.lastChunk;
        }
        return chunkNumber < other.chunkNumber;
    }
    return sequenceNumber < other.sequenceNumber;
}

bool SequenceData::operator==(const SequenceData& other) const
{
    return sequenceNumber == other.sequenceNumber && chunkNumber == other.chunkNumber && lastChunk == other.lastChunk;
}

bool SequenceData::operator!=(const SequenceData& other) const
{
    return !(*this == other);
}

}
