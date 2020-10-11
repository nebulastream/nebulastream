
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <vector>
#include <Util/Logger.hpp>
namespace NES{


TumblingWindow::TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size)
    :  WindowType(timeCharacteristic), size(size) {}

WindowTypePtr TumblingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size) {
    return std::make_shared<TumblingWindow>(TumblingWindow(timeCharacteristic, size));
}

uint64_t TumblingWindow::calculateNextWindowEnd(uint64_t currentTs) const  {
    return currentTs + size.getTime() - (currentTs % size.getTime());
}

void TumblingWindow::triggerWindows(std::vector<WindowState> &windows,
                                    uint64_t lastWatermark,
                                    uint64_t currentWatermark) const {
    NES_DEBUG("TumblingWindow::triggerWindows windows before=" << windows.size());
    long lastStart = lastWatermark - ((lastWatermark + size.getTime()) % size.getTime());
    NES_DEBUG("TumblingWindow::triggerWindows= lastStart=" << lastStart << " size.getTime()=" << size.getTime() << " lastWatermark=" << lastWatermark);
    for (long windowStart = lastStart; windowStart + size.getTime() <= currentWatermark; windowStart += size.getTime()) {
        NES_DEBUG("TumblingWindow::triggerWindows add window to be triggered = windowStart=" << windowStart);
        windows.emplace_back(windowStart, windowStart + size.getTime());
    }
    NES_DEBUG("TumblingWindow::triggerWindows windows after=" << windows.size());
}

bool TumblingWindow::isTumblingWindow() {
    return true;
}

TimeMeasure TumblingWindow::getSize() {
    return size;
}

}