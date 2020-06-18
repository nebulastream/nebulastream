
#include <Common/DataTypes/Array.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <utility>

namespace NES {

Array::Array(uint64_t length, DataTypePtr component) : length(length), component(std::move(component)) {}

DataTypePtr Array::getComponent() {
    return component;
}

uint64_t Array::getLength() const {
    return length;
}

bool Array::isArray() {
    return true;
}
bool Array::isEquals(DataTypePtr otherDataType) {
    if (otherDataType->isArray()) {
        auto otherArray = as<Array>(otherDataType);
        return length == otherArray->getLength() && component->isEquals(otherArray->getComponent());
    }
    return false;
}

DataTypePtr Array::join(DataTypePtr otherDataType) {
    return DataTypeFactory::createUndefined();
}

std::string Array::toString() {
    return "Array";
}

}// namespace NES