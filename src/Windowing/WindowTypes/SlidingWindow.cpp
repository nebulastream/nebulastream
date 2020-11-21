/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Util/Logger.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
namespace NES::Windowing {

SlidingWindow::SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : WindowType(timeCharacteristic), size(size), slide(slide) {}

WindowTypePtr SlidingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide) {
    return std::make_shared<SlidingWindow>(SlidingWindow(timeCharacteristic, size, slide));
}

void SlidingWindow::triggerWindows(std::vector<WindowState>& windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
    NES_DEBUG("SlidingWindow::triggerWindows windows before=" << windows.size());
    long lastStart = currentWatermark - ((currentWatermark + slide.getTime()) % slide.getTime());
    NES_DEBUG("SlidingWindow::triggerWindows= lastStart=" << lastStart << " size.getTime()=" << size.getTime() << " lastWatermark=" << lastWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() >= lastWatermark && windowStart >= 0; windowStart -= slide.getTime()) {
        if (windowStart >= 0 && ((windowStart + size.getTime()) < currentWatermark)) {//TODO we have to really find out if it is < or <= currentWatermark
            NES_DEBUG("SlidingWindow::triggerWindows add window to be triggered = windowStart=" << windowStart);
            windows.emplace_back(windowStart, windowStart + size.getTime());
        }
    }
}

uint64_t SlidingWindow::calculateNextWindowEnd(uint64_t currentTs) const {
    return currentTs + slide.getTime() - (currentTs % slide.getTime());
}

bool SlidingWindow::isSlidingWindow() {
    return true;
}

TimeMeasure SlidingWindow::getSize() {
    return size;
}

TimeMeasure SlidingWindow::getSlide() {
    return slide;
}

}// namespace NES::Windowing