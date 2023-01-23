#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_HASH_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_HASH_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus::Interface {
class HashFunction {
  public:
    using HashValue = Value<UInt64>;

    HashValue calculate(Value<>& value);
    HashValue calculate(std::vector<Value<>>& values);

  protected:
    virtual HashValue init() = 0;
    virtual HashValue calculate(HashValue& hash, Value<>& value) = 0;
};
}// namespace NES::Nautilus::Interface
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_HASH_HPP_
