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

#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp> //TODO 2891: forward declare
#include <sstream>

namespace NES::Windowing {

ThresholdWindow::ThresholdWindow(ExpressionNodePtr predicate) : WindowType(), predicate(std::move(predicate)) {}

WindowTypePtr ThresholdWindow::of(ExpressionNodePtr predicate) {
    return std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate)));
}

bool ThresholdWindow::equal(WindowTypePtr otherWindowType) {
    if (otherWindowType->isThresholdWindow()) {
        return std::dynamic_pointer_cast<ThresholdWindow>(otherWindowType)->getPredicate()->equal(this->getPredicate());
    } else {
        return false;
    }
}

std::string ThresholdWindow::toString() {
    std::stringstream ss;
    ss << "Threshold Window";
    ss << std::endl;
    return ss.str();
}
void ThresholdWindow::triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const {
    // TODO 2981: See how we should implement this/if it is required
    // hack: silence unused vars
    (void) windows;
    (void) lastWatermark;
    (void) currentWatermark;
}
TimeMeasure ThresholdWindow::getSize() {
    // TODO 2981: See how we should implement this/if it is required
    return TimeMeasure(0);
}

TimeMeasure ThresholdWindow::getSlide() {
    // TODO 2981: See how we should implement this/if it is required
    return TimeMeasure(0);
}

uint64_t ThresholdWindow::calculateNextWindowEnd(uint64_t currentTs) const {
    // TODO 2981: See how we should implement this/if it is required
    // hack: silence unused vars
    (void) currentTs;
    return 0;
}
bool ThresholdWindow::isThresholdWindow() { return true; }
const ExpressionNodePtr& ThresholdWindow::getPredicate() const { return predicate; }

}// namespace NES::Windowing