#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

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

}// namespace NES
