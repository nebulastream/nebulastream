#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <utility>

namespace NES::Windowing {

WindowType::WindowType(TimeCharacteristicPtr timeCharacteristic) : timeCharacteristic(std::move(timeCharacteristic)) {
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

}// namespace NES
