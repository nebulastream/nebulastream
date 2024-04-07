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

#ifndef NES_NES_COMMON_INCLUDE_SEQUENCING_SEQUENCEDATA_HPP_
#define NES_NES_COMMON_INCLUDE_SEQUENCING_SEQUENCEDATA_HPP_
#include <Identifiers.hpp>
#include <sstream>

namespace NES {

class SequenceData {
  public:
    /**
     * @brief Constructs
     * @param sequenceNumber
     * @param chunkNumber
     * @param lastChunk
     */
    SequenceData(SequenceNumber sequenceNumber, ChunkNumber chunkNumber, bool lastChunk);
    explicit SequenceData();

    [[nodiscard]] std::string toString() const;

    friend std::ostream& operator<<(std::ostream& os, const SequenceData& obj) {
        os << "{SeqNumber: " << obj.sequenceNumber << ", ChunkNumber: " << obj.chunkNumber << ", LastChunk: " << obj.lastChunk
           << "}";
        return os;
    }

    bool operator<=(const SequenceData& other) const;

    /**
     * @brief Overloading the < operator. Checks sequenceNumber, then chunkNumber, then lastChunk
     * @param other
     */
    bool operator<(const SequenceData& other) const;

    /**
     * @brief Overloading the == operator
     * @param other
     * @return True if both structs are equal
     */
    bool operator==(const SequenceData& other) const;

    /**
     * @brief Overloading the != operator
     * @param other
     * @return True if both structs are NOT equal
     */
    bool operator!=(const SequenceData& other) const;

    SequenceNumber sequenceNumber;
    ChunkNumber chunkNumber;
    bool lastChunk;
};

}// namespace NES

#endif//NES_NES_COMMON_INCLUDE_SEQUENCING_SEQUENCEDATA_HPP_
