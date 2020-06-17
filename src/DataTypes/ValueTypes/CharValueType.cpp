
#include <DataTypes/DataTypeFactory.hpp>
#include <DataTypes/ValueTypes/CharValueType.hpp>
#include <sstream>
namespace NES {

CharValueType::CharValueType(std::vector<std::string> values) : ValueType(DataTypeFactory::createFixedChar(values.size())), values(values) {}
CharValueType::CharValueType(const std::string& value) : ValueType(DataTypeFactory::createFixedChar(value.size())) {
    auto dimension = value.size();
    u_int32_t i = 0;
    std::stringstream str;
    values.push_back(value);
    /*

     * caused by while-loop it could make sense to include a maximum string-size here.
     * ERROR-POSSIBILITY: INFINITE-LOOP - CHARPOINTER WITHOUT PREDEFINED SIZE DOES NOT END WITH '\0'

    if (dimension == 0)
        while (value[i] != '\0') {
            values.push_back(std::to_string(value[i]));
            i++;
        }
    else {
        u_int32_t j;
        for (j = 0; j < dimension; j++) {
            values.push_back(std::to_string(value[j]));
            if (value[j] == '\0')
                break;
        }
        if (j == dimension) {
            values.push_back(std::to_string('\0'));
            j++;
        }
        i = j;
    }
    */
}

bool CharValueType::isCharValue() {
    return true;
}

bool CharValueType::isEquals(ValueTypePtr valueType) {
    return false;
}
std::string CharValueType::toString() {
    return std::string();
}
const std::vector<std::string>& CharValueType::getValues() const {
    return values;
}

}// namespace NES