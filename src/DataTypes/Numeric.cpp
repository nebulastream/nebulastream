
#include <DataTypes/Numeric.hpp>

namespace NES {

Numeric::Numeric(int8_t bits) : bits(bits){
}

bool Numeric::isNumeric() {
    return true;
}
int8_t Numeric::getBits() const {
    return bits;
}

}// namespace NES