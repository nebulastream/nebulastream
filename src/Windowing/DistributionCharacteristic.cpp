#include <API/AttributeField.hpp>
#include <Windowing/DistributionCharacteristic.hpp>

namespace NES::Windowing {

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

std::string DistributionCharacteristic::toString()
{
    if(type == Complete)
    {
        return "Complete";
    }
    else if(type == Slicing)
    {
        return "Slicing";
    }
    else if(type == Combining)
    {
        return "Combining";
    }
    else
    {
        return "";
    }
}

}// namespace NES::Windowing