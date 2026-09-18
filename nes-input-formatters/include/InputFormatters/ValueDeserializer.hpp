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

#include <cstdint>
#include <string>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <Arena.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Base class for all value deserializers
class ValueDeserializer
{
public:
    ValueDeserializer() noexcept = default;
    virtual ~ValueDeserializer() noexcept = default;

    /// Deserializes the value from serialized form to it's c++ type representation.
    /// Also performs null handling for the NullableValueDeserializer... implementations.
    /// If fieldAddress == nullptr and fieldSize == 0, the field is deemed as not found
    /// A deserializer that cannot represent the value as a view into the raw buffer, because it has to decode or assemble the value
    /// first, allocates the memory for it from the arena. The arena's memory is reclaimed after the pipeline invocation, which is
    /// after the record has been written into a tuple buffer.
    [[nodiscard]] virtual VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena) const
        = 0;
};
}
