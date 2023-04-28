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

#ifndef NES_NLJWINDOW_HPP
#define NES_NLJWINDOW_HPP

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace NES::Runtime::Execution {

class NLJWindow {
public:

    enum class WindowState : uint8_t { BOTH_SIDES_FILLING, ONLY_LEFT_FILLING, ONLY_RIGHT_FILLING, DONE_FILLING,
            EMITTED_TO_NLJ_SINK};

    explicit NLJWindow(uint64_t windowStart, uint64_t windowEnd);

    bool operator==(const NLJWindow &rhs) const;
    bool operator!=(const NLJWindow &rhs) const;

    /**
     * @brief Makes sure that enough space is available for writing the tuple. This method returns a pointer to the start
     * of the newly space
     * @param sizeOfTupleInByte
     * @return Pointer to start of memory space
     */
    uint8_t* insertNewTuple(size_t sizeOfTupleInByte, bool leftSide);

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

    uint64_t getWindowStart() const;

    uint64_t getWindowEnd() const;

    std::atomic<WindowState> windowState;

private:
    std::vector<uint8_t> leftTuples;
    std::vector<uint8_t> rightTuples;
    std::mutex leftTuplesMutex;
    std::mutex rightTuplesMutex;
    uint64_t windowStart;
    uint64_t windowEnd;

};
} // namespace NES::Runtime::Execution

#endif //NES_NLJWINDOW_HPP
