#include <Util/Logger.hpp>
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
    for (long windowStart = lastStart; windowStart + size.getTime() > lastWatermark; windowStart -= slide.getTime()) {
        if (windowStart >= 0 && ((windowStart + size.getTime()) <= currentWatermark)) {
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

}// namespace NES