#include <API/Window/TimeCharacteristic.hpp>

namespace NES{

TimeCharacteristic::TimeCharacteristic(Type type): type(type) {}
TimeCharacteristic::TimeCharacteristic(Type type, AttributeFieldPtr field): type(type), field(field) {}

TimeCharacteristicPtr TimeCharacteristic::createEventTime(AttributeFieldPtr field) {
    return std::make_shared<TimeCharacteristic>(Type::EventTime, field);
}

TimeCharacteristicPtr TimeCharacteristic::createProcessingTime() {
    return std::make_shared<TimeCharacteristic>(Type::ProcessingTime);
}

AttributeFieldPtr TimeCharacteristic::getField() {
    return field;
}

TimeCharacteristic::Type TimeCharacteristic::getType() {
    return type;
}


}