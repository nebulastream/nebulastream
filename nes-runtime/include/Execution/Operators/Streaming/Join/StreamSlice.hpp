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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMSLICE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMSLICE_HPP_
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {
/**
 * @brief This class represents a single slice for a join. It stores all values for the left and right stream.
 */
class StreamSlice {
  public:
    /**
     * @brief Constructor for creating a slice
     * @param sliceStart: Start timestamp of this slice
     * @param sliceEnd: End timestamp of this slice
     */
    explicit StreamSlice(uint64_t sliceStart, uint64_t sliceEnd);

    /**
     * @brief Compares if two slices are equal
     * @param rhs
     * @return Boolean
     */
    bool operator==(const StreamSlice& rhs) const;

    /**
     * @brief Compares if two slices are NOT equal
     * @param rhs
     * @return Boolean
     */
    bool operator!=(const StreamSlice& rhs) const;

    /**
     * @brief Getter for the start ts of the slice
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getSliceStart() const;

    /**
     * @brief Getter for the end ts of the slice
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getSliceEnd() const;

    /**
     * @brief Returns the number of tuples in this slice for the left side
     * @return uint64_t
     */
    virtual uint64_t getNumberOfTuplesLeft() = 0;

    /**
     * @brief Returns the number of tuples in this slice for the right side
     * @return uint64_t
     */
    virtual uint64_t getNumberOfTuplesRight() = 0;

    /**
     * @brief Returns the identifier for this slice. For now, the identifier is the sliceEnd
     * @return uint64_t
     */
    uint64_t getSliceIdentifier() const;

    /**
     * @brief Static method for getting the identifier from sliceStart and SliceEnd
     * @param sliceStart
     * @param sliceEnd
     * @return uint64_t
     */
    static uint64_t getSliceIdentifier(uint64_t sliceStart, uint64_t sliceEnd);

    /**
     * @brief Creates a string representation of this slice
     * @return String
     */
    virtual std::string toString() = 0;

    /**
     * @brief Virtual deconstructor
     */
    virtual ~StreamSlice() = default;

  protected:
    uint64_t sliceStart;
    uint64_t sliceEnd;
};
}// namespace NES::Runtime::Execution

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMSLICE_HPP_
