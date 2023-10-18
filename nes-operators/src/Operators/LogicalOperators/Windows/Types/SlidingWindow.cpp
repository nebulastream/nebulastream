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

#include <API/AttributeField.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowState.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <utility>

namespace NES::Windowing {

SlidingWindow::SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size)), slide(std::move(slide)) {}

WindowTypePtr SlidingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide) {
    return std::make_shared<SlidingWindow>(SlidingWindow(std::move(timeCharacteristic), std::move(size), std::move(slide)));
}

void SlidingWindow::triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const {
    NES_TRACE("SlidingWindow::triggerWindows windows before={}", windows.size());
    long lastStart = currentWatermark - ((currentWatermark + slide.getTime()) % slide.getTime());
    NES_TRACE("SlidingWindow::triggerWindows= lastStart={} size.getTime()={} lastWatermark={}",
              lastStart,
              size.getTime(),
              lastWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() > lastWatermark && windowStart >= 0;
         windowStart -= slide.getTime()) {
        if (windowStart >= 0 && ((windowStart + size.getTime()) <= currentWatermark)) {
            NES_TRACE("SlidingWindow::triggerWindows add window to be triggered = windowStart={}", windowStart);
            windows.emplace_back(windowStart, windowStart + size.getTime());
        }
    }
}

uint64_t SlidingWindow::calculateNextWindowEnd(uint64_t currentTs) const {
    return currentTs + slide.getTime() - (currentTs % slide.getTime());
}

TimeBasedWindowType::TimeBasedSubWindowType SlidingWindow::getTimeBasedSubWindowType() { return SLIDINGWINDOW; }

TimeMeasure SlidingWindow::getSize() { return size; }

TimeMeasure SlidingWindow::getSlide() { return slide; }

std::string SlidingWindow::toString() {
    std::stringstream ss;
    ss << "SlidingWindow: size=" << size.getTime();
    ss << " slide=" << slide.getTime();
    ss << " timeCharacteristic=" << timeCharacteristic->toString();
    ss << std::endl;
    return ss.str();
}

bool SlidingWindow::equal(WindowTypePtr otherWindowType) {
    if (auto otherSlidingWindow = std::dynamic_pointer_cast<SlidingWindow>(otherWindowType)) {
        return this->size.equals(otherSlidingWindow->size) && this->slide.equals(otherSlidingWindow->slide)
            && this->timeCharacteristic->equals(*otherSlidingWindow->timeCharacteristic);
    }
    return false;
}

}// namespace NES::Windowing