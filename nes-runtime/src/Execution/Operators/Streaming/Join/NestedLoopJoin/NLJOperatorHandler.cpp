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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

bool NLJOperatorHandler::updateStateOfNLJWindows(uint64_t timestamp, bool isLeftSide) {
    bool atLeastOneDoneFilling = false;

    for (auto& curWindow : nljWindows) {
        if (curWindow.windowState == NLJWindow::WindowState::DONE_FILLING) {
            atLeastOneDoneFilling = true;
            continue;
        }
        if (curWindow.windowState == NLJWindow::WindowState::EMITTED_TO_NLJ_SINK ||
            (isLeftSide && curWindow.windowState == NLJWindow::WindowState::ONLY_RIGHT_FILLING) ||
            (!isLeftSide && curWindow.windowState == NLJWindow::WindowState::ONLY_LEFT_FILLING)) {
            continue;
        }

        if (timestamp > curWindow.getWindowEnd()) {
            if ((isLeftSide && curWindow.windowState == NLJWindow::WindowState::ONLY_RIGHT_FILLING) ||
                (!isLeftSide && curWindow.windowState == NLJWindow::WindowState::ONLY_LEFT_FILLING)) {
                curWindow.windowState = NLJWindow::WindowState::DONE_FILLING;
                atLeastOneDoneFilling = true;
            }

            if (curWindow.windowState == NLJWindow::WindowState::BOTH_SIDES_FILLING) {
                if (isLeftSide) {
                    curWindow.windowState = NLJWindow::WindowState::ONLY_RIGHT_FILLING;
                } else {
                    curWindow.windowState = NLJWindow::WindowState::ONLY_LEFT_FILLING;
                }
            }
        }
    }

    return atLeastOneDoneFilling;
}

std::list<NLJWindow> &NLJOperatorHandler::getAllNLJWindows() {
    return nljWindows;
}
} // namespace NES::Runtime::Execution::Operators