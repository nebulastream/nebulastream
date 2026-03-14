/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#pragma once
#include <memory>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES
{

/// Implementation of the XXH3-64 hash function for nautilus types.
/// XXH3 is the latest variant of the xxHash family, optimized for speed on both small and large inputs.
/// This is a self-contained implementation based on the public domain xxHash algorithm by Yann Collet.
class XXH3HashFunction : public HashFunction
{
public:
    [[nodiscard]] HashValue init() const override;

    [[nodiscard]] std::unique_ptr<HashFunction> clone() const override;

protected:
    /// Calculates the XXH3-64 hash of value and combines it with hash via XOR.
    [[nodiscard]] HashValue calculate(HashValue& hash, const VarVal& value) const override;
};
}
