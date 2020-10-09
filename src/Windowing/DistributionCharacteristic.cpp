#include <API/AttributeField.hpp>
#include <Windowing/DistributionCharacteristic.hpp>

namespace NES {

DistributionCharacteristic::DistributionCharacteristic(Type type) : type(type) {}

DistributionCharacteristicPtr DistributionCharacteristic::createCompleteWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Complete);
}

DistributionCharacteristicPtr DistributionCharacteristic::createSlicingWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Slicing);
}

DistributionCharacteristicPtr DistributionCharacteristic::createCombiningWindowType() {
    return std::make_shared<DistributionCharacteristic>(Type::Combining);
}

DistributionCharacteristic::Type DistributionCharacteristic::getType() {
    return type;
}

}// namespace NES