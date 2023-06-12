
#include <Nautilus/Interface/DataTypes/Numeric/Numeric.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdio>
#include <memory>

namespace NES::Nautilus {

static constexpr int64_t numericShifts[19] = {1ll,
                                              10ll,
                                              100ll,
                                              1000ll,
                                              10000ll,
                                              100000ll,
                                              1000000ll,
                                              10000000ll,
                                              100000000ll,
                                              1000000000ll,
                                              10000000000ll,
                                              100000000000ll,
                                              1000000000000ll,
                                              10000000000000ll,
                                              100000000000000ll,
                                              1000000000000000ll,
                                              10000000000000000ll,
                                              100000000000000000ll,
                                              1000000000000000000ll};

Numeric::Numeric(int8_t precision, int64_t value) : Numeric(precision, Value<Int64>(value)) {}

Numeric::Numeric(int8_t precision, Value<Int64> value) : Any(&type), precision(precision), value(value) {}

Value<Numeric> Numeric::add(Value<Numeric>& other) {
    assert(precision == other->precision);
    Value newValue = value + other->value;
    auto numeric = create<Numeric>(precision, newValue.as<Int64>());
    return Value<Numeric>(numeric);
}

Value<Numeric> Numeric::sub(Value<NES::Nautilus::Numeric>& other) {
    assert(precision == other->precision);
    Value newValue = value - other->value;
    auto numeric = create<Numeric>(precision, newValue.as<Int64>());
    return Value<Numeric>(numeric);
}

Value<Numeric> Numeric::div(Value<NES::Nautilus::Numeric>& other) {
    auto power = numericShifts[precision];
    auto newValue = value * power / other->value;
    auto numeric = create<Numeric>(precision, newValue.as<Int64>());
    return Value<Numeric>(numeric);
}

Value<Numeric> Numeric::mul(Value<NES::Nautilus::Numeric>& other) {
    auto resultPrecision = precision + other->precision;
    Value newValue = value * other->value;
    auto numeric = create<Numeric>(resultPrecision, newValue.as<Int64>());
    return Value<Numeric>(numeric);
}

Value<Int64> Numeric::getValue() {
    return value;
}

Value<> Numeric::equals(Value<NES::Nautilus::Numeric> other) { return value == other->value; }

Value<> Numeric::lessThan(Value<NES::Nautilus::Numeric> other) { return value < other->value; }

Value<> Numeric::greaterThan(Value<NES::Nautilus::Numeric> other) {
    return value > other->value;
}

std::string Numeric::toString() { return ""; };

std::shared_ptr<Any> Numeric::copy() { return create<Numeric>(precision, value); }

Nautilus::IR::Types::StampPtr Numeric::getType() const { return value->getType(); }

}// namespace NES::Nautilus