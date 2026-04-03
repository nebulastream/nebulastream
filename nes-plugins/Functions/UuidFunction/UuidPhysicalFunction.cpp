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

#include <Functions/PhysicalFunctionPluginMacros.hpp>

#include <bit>
#include <cstdint>
#include <cstring>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <uuid/uuid.h>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

/// Converts two UINT64 values (high bits, low bits) into a UUID string using libuuid.
/// Output format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars + null terminator).
NES_PHYSICAL_FUNCTION(2, Uuid)
{
    const auto highValue = childFunctions[0].execute(record, arena);
    const auto lowValue = childFunctions[1].execute(record, arena);

    const auto isNullable = highValue.isNullable() or lowValue.isNullable();
    if (isNullable and (highValue.isNull() or lowValue.isNull()))
    {
        return VarVal{VariableSizedData{nullptr, 0}, true, true};
    }

    const auto high = highValue.getRawValueAs<nautilus::val<uint64_t>>();
    const auto low = lowValue.getRawValueAs<nautilus::val<uint64_t>>();

    /// 36 chars + null terminator
    constexpr uint32_t uuidStringLen = 37;
    auto uuidData = arena.allocateVariableSizedData(nautilus::val<uint32_t>{uuidStringLen});
    const auto payload = uuidData.getContent();

    nautilus::invoke(
        +[](const uint64_t highBits, const uint64_t lowBits, char* out)
        {
            /// uuid_t is uint8_t[16] in big-endian byte order
            const auto highBE = std::endian::native == std::endian::big ? highBits : std::byteswap(highBits);
            const auto lowBE = std::endian::native == std::endian::big ? lowBits : std::byteswap(lowBits);
            uuid_t uuid;
            std::memcpy(uuid, &highBE, 8);
            std::memcpy(uuid + 8, &lowBE, 8);
            uuid_unparse_lower(uuid, out);
        },
        high,
        low,
        payload);

    return VarVal{uuidData, isNullable, false};
}

}
