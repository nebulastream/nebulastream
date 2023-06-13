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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INVOCATIONPLUGIN_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INVOCATIONPLUGIN_HPP_
#include <Nautilus/IR/Types/Stamp.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Util/PluginRegistry.hpp>
#include <optional>
namespace NES::Nautilus {

class InvocationPlugin {
  public:
    InvocationPlugin() = default;
    virtual ~InvocationPlugin() = default;
    virtual std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Sub(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Mul(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Div(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Mod(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Negate(const Value<>& left) const;
    virtual std::optional<Value<>> And(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> Or(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> BitWiseAnd(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> BitWiseOr(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> BitWiseXor(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> BitWiseLeftShift(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> BitWiseRightShift(const Value<>& left, const Value<>& right) const;
    virtual std::optional<Value<>> CastTo(const Value<>& left, const TypeIdentifier* toType) const;
    virtual std::optional<Value<>> ReadArrayIndex(const Value<>& array, Value<UInt32> index) const;
    virtual std::optional<Value<>> WriteArrayIndex(const Value<>& array, Value<UInt32> index, const Value<>& value) const;
    virtual bool IsCastable(const Value<>&, const TypeIdentifier*) const;
};

using InvocationPluginRegistry = Util::PluginRegistry<InvocationPlugin>;

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_INVOCATIONPLUGIN_HPP_
