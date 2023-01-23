#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_MURMUR3HASHFUNCTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_MURMUR3HASHFUNCTION_HPP_
#include <Execution/DataStructures/Hash/HashFunction.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus::Interface {
class MurMur3HashFunction : public HashFunction {
  protected:
    const uint64_t SEED = 902850234;
    HashValue init() override;
    HashValue calculate(HashValue& hash, Value<>& value) override;
};
}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_HASH_MURMUR3HASHFUNCTION_HPP_
