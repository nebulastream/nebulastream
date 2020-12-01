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
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <vector>

namespace NES::Windowing {

TumblingWindow::TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size)
    : WindowType(timeCharacteristic), size(size) {}

WindowTypePtr TumblingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size) {
    return std::make_shared<TumblingWindow>(TumblingWindow(timeCharacteristic, size));
}

uint64_t TumblingWindow::calculateNextWindowEnd(uint64_t currentTs) const {
    return currentTs + size.getTime() - (currentTs % size.getTime());
}

void TumblingWindow::triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const {
    NES_DEBUG("TumblingWindow::triggerWindows windows before=" << windows.size());
    //lastStart = last window that starts before the watermark
    long lastStart = lastWatermark - ((lastWatermark + size.getTime()) % size.getTime());
    NES_DEBUG("TumblingWindow::triggerWindows= lastStart=" << lastStart << " size.getTime()=" << size.getTime()
                                                           << " lastWatermark=" << lastWatermark
                                                           << " currentWatermark=" << currentWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() <= currentWatermark; windowStart += size.getTime()) {
        NES_DEBUG("TumblingWindow::triggerWindows  add window start =" << windowStart << " end=" << windowStart + size.getTime());
        windows.emplace_back(windowStart, windowStart + size.getTime());
    }
    NES_DEBUG("TumblingWindow::triggerWindows windows after=" << windows.size());
}

bool TumblingWindow::isTumblingWindow() { return true; }

TimeMeasure TumblingWindow::getSize() { return size; }

std::string TumblingWindow::toString() {
    std::stringstream ss;
    ss << "TumblingWindow: size=" << size.getTime();
    ss << " timeCharacteristic=" << timeCharacteristic->toString();
    ss << std::endl;
    return ss.str();
}

}// namespace NES::Windowing