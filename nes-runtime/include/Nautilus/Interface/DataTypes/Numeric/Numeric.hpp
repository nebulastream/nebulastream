
#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_NUMERIC_NUMERIC_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_NUMERIC_NUMERIC_HPP_

#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <cstdint>

namespace NES::Nautilus {

/**
* @brief A Numeric data type with a value and a fixed precision
*/
class Numeric : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<Numeric>();

    Numeric(int8_t precision, int64_t value);
    Numeric(int8_t precision, Value<Int64> value);

    Value<Numeric> add(Value<Numeric>& other);
    Value<Numeric> sub(Value<Numeric>& other);
    Value<Numeric> div(Value<Numeric>& other);
    Value<Numeric> mul(Value<Numeric>& other);
    Value<> equals(Value<Numeric> other);
    Value<> lessThan(Value<Numeric> other);
    Value<> greaterThan(Value<Numeric> other);

    std::string toString() override;

    Nautilus::IR::Types::StampPtr getType() const override;

    std::shared_ptr<Any> copy() override;
    Value<Int64> getValue();
  private:
    uint8_t precision;
    Value<Int64> value;
};
}// namespace NES::Nautilus

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_NUMERIC_NUMERIC_HPP_
