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
#include <sstream>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

struct SequenceData
{
    SequenceData(SequenceNumber sequenceNumber, ChunkNumber chunkNumber, bool lastChunk);
    explicit SequenceData();

    friend std::ostream& operator<<(std::ostream& os, const SequenceData& obj);

    bool operator<=(const SequenceData& other) const;
    /// Checks sequenceNumber, then chunkNumber, then lastChunk
    bool operator<(const SequenceData& other) const;
    bool operator==(const SequenceData& other) const;
    bool operator!=(const SequenceData& other) const;

    SequenceNumber::Underlying sequenceNumber;
    ChunkNumber::Underlying chunkNumber;
    bool lastChunk;
};

}
FMT_OSTREAM(NES::SequenceData);
