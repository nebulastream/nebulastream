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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>
namespace NES::Runtime::Execution {

    uint8_t* NLJWindow::insertNewTuple(size_t sizeOfTupleInByte, bool leftSide) {
        if (leftSide) {
            std::lock_guard<std::mutex> lock(leftTuplesMutex);
            auto currentSize = leftTuples.size();
            leftTuples.resize(currentSize + sizeOfTupleInByte);
            return &leftTuples[currentSize];
        } else {
            std::lock_guard<std::mutex> lock(rightTuplesMutex);
            auto currentSize = rightTuples.size();
            rightTuples.resize(currentSize + sizeOfTupleInByte);
            return &rightTuples[currentSize];
        }
    }

    uint8_t* NLJWindow::getTuple(size_t sizeOfTupleInByte, size_t tuplePos, bool leftSide) {
        if (leftSide) {
            return &leftTuples[sizeOfTupleInByte * tuplePos];
        } else {
            return &rightTuples[sizeOfTupleInByte * tuplePos];
        }
    }

    size_t NLJWindow::getNumberOfTuples(size_t sizeOfTupleInByte, bool leftSide) {
        if (leftSide) {
            return leftTuples.size() / sizeOfTupleInByte;
        } else {
            return rightTuples.size() / sizeOfTupleInByte;
        }
    }

    uint64_t NLJWindow::getWindowStart() const {
        return windowStart;
    }

    uint64_t NLJWindow::getWindowEnd() const {
        return windowEnd;
    }

    NLJWindow::NLJWindow(uint64_t windowStart, uint64_t windowEnd) : windowState(WindowState::BOTH_SIDES_FILLING),
    windowStart(windowStart), windowEnd(windowEnd) {}

    bool NLJWindow::operator==(const NLJWindow &rhs) const {
        return windowState == rhs.windowState &&
               leftTuples == rhs.leftTuples &&
               rightTuples == rhs.rightTuples &&
               windowStart == rhs.windowStart &&
               windowEnd == rhs.windowEnd;
    }

    bool NLJWindow::operator!=(const NLJWindow &rhs) const {
        return !(rhs == *this);
    }

    NLJWindow::WindowState NLJWindow::getWindowState() const {
        return windowState.load();
    }

    void NLJWindow::updateWindowState(NLJWindow::WindowState newWindowState) {
        NES_DEBUG2("Changing windowState for {} to {}", toString(), magic_enum::enum_name(newWindowState));
        windowState.store(newWindowState);
    }

    bool NLJWindow::compareCurrentWindowState(NLJWindow::WindowState expectedState) {
        return getWindowState() == expectedState;
    }
    bool NLJWindow::compareExchangeStrong(NLJWindow::WindowState expectedState, NLJWindow::WindowState newWindowState) {
        return windowState.compare_exchange_strong(expectedState, newWindowState);
    }
    std::string NLJWindow::toString() {
        std::ostringstream basicOstringstream;
        basicOstringstream << "(windowState: " << magic_enum::enum_name(windowState.load())
                           << " windowStart: " << windowStart << " windowEnd: " << windowEnd << ")";
        return basicOstringstream.str();
    }

    }