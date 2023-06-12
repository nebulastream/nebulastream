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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_

#include <atomic>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <vector>

namespace NES::Runtime::Execution {

/**
 * @brief This class represents a single window for the nested loop join. It stores all values for the left and right stream.
 * Later on this class can be reused for a slice.
 */
class NLJWindow {
  public:
    enum class WindowState : uint8_t { BOTH_SIDES_FILLING, EMITTED_TO_NLJ_SINK };

    /**
     * @brief Constructor for creating a window
     * @param windowStart
     * @param windowEnd
     */
    explicit NLJWindow(uint64_t windowStart, uint64_t windowEnd);

    /**
     * @brief Compares if two windows are equal
     * @param rhs
     * @return Boolean
     */
    bool operator==(const NLJWindow& rhs) const;

    /**
     * @brief Compares if two windows are NOT equal
     * @param rhs
     * @return Boolean
     */
    bool operator!=(const NLJWindow& rhs) const;

    /**
     * @brief Makes sure that enough space is available for writing the tuple. This method returns a pointer to the start
     * of the newly space
     * @param sizeOfTupleInByte
     * @return Pointer to start of memory space
     */
    uint8_t* allocateNewTuple(size_t sizeOfTupleInByte, bool leftSide);

    /**
     * @brief Returns the
     * @param sizeOfTupleInByte
     * @param tuplePos
     * @param leftSide
     * @return Pointer to the start of the memory for the
     */
    uint8_t* getTuple(size_t sizeOfTupleInByte, size_t tuplePos, bool leftSide);

    /**
     * @brief Returns the number of tuples in this window
     * @param sizeOfTupleInByte
     * @param leftSide
     * @return size_t
     */
    size_t getNumberOfTuples(size_t sizeOfTupleInByte, bool leftSide);

    /**
     * @brief Getter for the start ts of the window
     * @return uint64_t
     */
    uint64_t getWindowStart() const;

    /**
     * @brief Getter for the end ts of the window
     * @return uint64_t
     */
    uint64_t getWindowEnd() const;

    /**
     * @brief Returns the identifier for this window. For now, the identifier is the windowEnd
     * @return uint64_t
     */
    uint64_t getWindowIdentifier() const;

    /**
     * @brief Wrapper for std::atomic<T>::compare_exchange_strong
     * @param expectedState
     * @param newWindowState
     * @return Bool
     */
    bool compareExchangeStrong(WindowState expectedState, WindowState newWindowState);

    /**
     * @brief Creates a string representation of this window
     * @return String
     */
    std::string toString();

  private:
    std::atomic<WindowState> windowState;
    std::vector<uint8_t> leftTuples;
    std::vector<uint8_t> rightTuples;
    std::mutex leftTuplesMutex;
    std::mutex rightTuplesMutex;
    uint64_t windowStart;
    uint64_t windowEnd;
};
}// namespace NES::Runtime::Execution

#endif // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_DATASTRUCTURE_NLJWINDOW_HPP_
