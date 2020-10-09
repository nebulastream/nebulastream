#include <Windowing/WindowMeasures/WindowMeasure.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
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

}// namespace NES
