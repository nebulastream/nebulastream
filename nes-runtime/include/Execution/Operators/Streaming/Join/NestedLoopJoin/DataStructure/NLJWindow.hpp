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

#include <cstdint>
#include <mutex>
#include <vector>

namespace NES::Runtime::Execution {
class NLJWindow {

    /**
     * @brief Makes sure that enough space is available for writing the tuple. This method returns a pointer to the start
     * of the newly space
     * @param sizeOfTupleInByte
     * @return Pointer to start of memory space
     */
    uint8_t* insertNewTuple(size_t sizeOfTupleInByte, bool leftSide);

private:
    std::vector<uint8_t> leftTuples;
    std::vector<uint8_t> rightTuples;
    std::mutex leftTuplesMutex;
    std::mutex rightTuplesMutex;
};
} // namespace NES::Runtime::Execution

#endif //NES_NLJWINDOW_HPP
