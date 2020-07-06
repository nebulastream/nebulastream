#include <API/Window/WindowMeasure.hpp>
#include <API/Window/WindowType.hpp>
#include <Util/Logger.hpp>
#include <memory>

namespace NES {

WindowType::WindowType(TimeCharacteristicPtr timeCharacteristic) : timeCharacteristic(timeCharacteristic) {
}

TimeCharacteristicPtr WindowType::getTimeCharacteristic() const {
    return this->timeCharacteristic;
}

TumblingWindow::TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size)
    : size(size), WindowType(timeCharacteristic) {}

WindowTypePtr TumblingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size) {
    return std::make_shared<TumblingWindow>(TumblingWindow(timeCharacteristic, size));
}

void TumblingWindow::triggerWindows(WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
    long lastStart = lastWatermark - ((lastWatermark + size.getTime()) % size.getTime());
    for (long windowStart = lastStart; windowStart + size.getTime() <= currentWatermark;
         windowStart += size.getTime()) {
        windows->emplace_back(windowStart, windowStart + size.getTime());
    }
}

SlidingWindow::SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : size(size), slide(slide), WindowType(timeCharacteristic) {
    NES_NOT_IMPLEMENTED();
}

WindowTypePtr SlidingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide) {
    return std::make_shared<SlidingWindow>(SlidingWindow(timeCharacteristic, size, slide));
}

void SlidingWindow::triggerWindows(WindowListPtr windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
    NES_NOT_IMPLEMENTED();
}

SessionWindow::SessionWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure gap)
    : gap(gap), WindowType(timeCharacteristic) {
    NES_NOT_IMPLEMENTED();
}

WindowTypePtr SessionWindow::withGap(TimeCharacteristicPtr timeCharacteristic, TimeMeasure gap) {
    return std::make_shared<SessionWindow>(SessionWindow(timeCharacteristic, gap));
}

void SessionWindow::triggerWindows(WindowListPtr windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
    NES_NOT_IMPLEMENTED();
}

WindowState::WindowState(uint64_t start, uint64_t an_end) : start(start), end(an_end) {}

uint64_t WindowState::getStartTs() const {
    return start;
}
uint64_t WindowState::getEndTs() const {
    return end;
}
}// namespace NES
