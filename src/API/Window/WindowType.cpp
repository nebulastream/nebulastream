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

bool WindowType::isSlidingWindow() {
    return false;
}

bool WindowType::isTumblingWindow() {
    return false;
}

bool WindowType::isSessionWindow() {
    return false;
}

TumblingWindow::TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size)
    : size(size), WindowType(timeCharacteristic) {}

WindowTypePtr TumblingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size) {
    return std::make_shared<TumblingWindow>(TumblingWindow(timeCharacteristic, size));
}

uint64_t TumblingWindow::getTime() const {
    return size.getTime();
}

void TumblingWindow::triggerWindows(WindowListPtr windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
    NES_DEBUG("TumblingWindow::triggerWindows windows before=" << windows->size());
    long lastStart = lastWatermark - ((lastWatermark + size.getTime()) % size.getTime());
    NES_DEBUG("TumblingWindow::triggerWindows= lastStart=" << lastStart << " size.getTime()=" << size.getTime() << " lastWatermark=" << lastWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() <= currentWatermark; windowStart += size.getTime()) {
        NES_DEBUG("TumblingWindow::triggerWindows add window to be triggered = windowStart=" << windowStart);
        windows->emplace_back(windowStart, windowStart + size.getTime());
    }
    NES_DEBUG("TumblingWindow::triggerWindows windows after=" << windows->size());
}

bool TumblingWindow::isTumblingWindow() {
    return true;
}

TimeMeasure TumblingWindow::getSize() {
    return size;
}

uint64_t SlidingWindow::getSizeTime() const {
    return size.getTime();
}

uint64_t SlidingWindow::getSlideTime() const {
    return slide.getTime();
}

SlidingWindow::SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : size(size), slide(slide), WindowType(timeCharacteristic) {}

WindowTypePtr SlidingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide) {
    return std::make_shared<SlidingWindow>(SlidingWindow(timeCharacteristic, size, slide));
}

uint64_t SlidingWindow::getTime() const {
    return size.getTime();
}

void SlidingWindow::triggerWindows(WindowListPtr windows,
                                   uint64_t lastWatermark,
                                   uint64_t currentWatermark) const {
    NES_DEBUG("SlidingWindow::triggerWindows windows before=" << windows->size());
    long lastStart = currentWatermark - ((currentWatermark + slide.getTime()) % slide.getTime());
    NES_DEBUG("SlidingWindow::triggerWindows= lastStart=" << lastStart << " size.getTime()=" << size.getTime() << " lastWatermark=" << lastWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() > lastWatermark; windowStart -= slide.getTime()) {
        if (windowStart >= 0 && ((windowStart + size.getTime()) <= currentWatermark)) {
            NES_DEBUG("SlidingWindow::triggerWindows add window to be triggered = windowStart=" << windowStart);
            windows->emplace_back(windowStart, windowStart + size.getTime());
        }
    }
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

SessionWindow::SessionWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure gap)
    : gap(gap), WindowType(timeCharacteristic) {
    NES_NOT_IMPLEMENTED();
}

WindowTypePtr SessionWindow::withGap(TimeCharacteristicPtr timeCharacteristic, TimeMeasure gap) {
    return std::make_shared<SessionWindow>(SessionWindow(timeCharacteristic, gap));
}

void SessionWindow::triggerWindows(WindowListPtr,
                                   uint64_t,
                                   uint64_t) const {
    NES_NOT_IMPLEMENTED();
}

uint64_t SessionWindow::getTime() const {
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
