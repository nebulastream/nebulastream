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
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

std::optional<Value<>> InvocationPlugin::Add(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Mul(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Sub(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Div(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Mod(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Equals(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::LessThan(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::GreaterThan(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Negate(const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::And(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::Or(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::BitWiseAnd(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::BitWiseOr(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::BitWiseXor(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::BitWiseLeftShift(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::BitWiseRightShift(const Value<>&, const Value<>&) const { return std::nullopt; }
std::optional<Value<>> InvocationPlugin::CastTo(const Value<>&, const TypeIdentifier*) const { return std::nullopt; }
bool InvocationPlugin::IsCastable(const Value<>&, const TypeIdentifier*) const { return false; }
std::optional<Value<>> InvocationPlugin::WriteArrayIndex(const Value<>&, Value<UInt32>, const Value<>&) const {
    return std::nullopt;
}
std::optional<Value<>> InvocationPlugin::ReadArrayIndex(const Value<>&, Value<UInt32>) const { return std::nullopt; }
}// namespace NES::Nautilus