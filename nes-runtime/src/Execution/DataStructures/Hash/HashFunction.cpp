#include <Execution/DataStructures/Hash/HashFunction.hpp>

namespace NES::Nautilus::Interface {
HashFunction::HashValue HashFunction::calculate(Value<>& value) {
    auto hash = init();
    return calculate(hash, value);
};

HashFunction::HashValue HashFunction::calculate(std::vector<Value<>>& values) {
    auto hash = init();
    for (auto& value : values) {
        hash = calculate(hash, value);
    }
    return hash;
}
}// namespace NES::Nautilus::Interface