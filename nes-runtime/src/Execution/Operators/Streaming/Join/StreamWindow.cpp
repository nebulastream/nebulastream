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

#include <Execution/Operators/Streaming/Join/StreamWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <sstream>

namespace NES::Runtime::Execution {

uint64_t StreamWindow::getWindowIdentifier() const { return getWindowEnd(); }

uint64_t StreamWindow::getWindowStart() const { return windowStart; }

uint64_t StreamWindow::getWindowEnd() const { return windowEnd; }

bool StreamWindow::isAlreadyEmitted() const { return windowState == StreamWindow::WindowState::EMITTED_TO_PROBE; }

StreamWindow::StreamWindow(uint64_t windowStart, uint64_t windowEnd)
    : windowState(WindowState::BOTH_SIDES_FILLING), windowStart(windowStart), windowEnd(windowEnd) {}

bool StreamWindow::operator==(const StreamWindow& rhs) const {
    return windowState == rhs.windowState && windowStart == rhs.windowStart && windowEnd == rhs.windowEnd;
}

bool StreamWindow::operator!=(const StreamWindow& rhs) const { return !(rhs == *this); }

bool StreamWindow::compareExchangeStrong(StreamWindow::WindowState expectedState, StreamWindow::WindowState newWindowState) {
    return windowState.compare_exchange_strong(expectedState, newWindowState);
}

bool StreamWindow::shouldTriggerDuringTerminate() {
    std::lock_guard lock{triggerTerminationMutex};
    if (windowState == StreamWindow::WindowState::ONCE_SEEN_DURING_TERMINATION) {
        windowState = StreamWindow::WindowState::EMITTED_TO_PROBE;
        return true;
    } else if (windowState == StreamWindow::WindowState::BOTH_SIDES_FILLING) {
        windowState = StreamWindow::WindowState::ONCE_SEEN_DURING_TERMINATION;
    }

    return false;
}

std::string StreamWindow::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "(windowState: " << magic_enum::enum_name(windowState.load()) << " windowStart: " << windowStart
                       << " windowEnd: " << windowEnd << ")";
    return basicOstringstream.str();
}

}// namespace NES::Runtime::Execution