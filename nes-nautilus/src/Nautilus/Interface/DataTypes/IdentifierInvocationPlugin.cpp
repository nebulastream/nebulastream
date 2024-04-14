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
#include <functional>
namespace NES::Nautilus {

class IdentifierInvocationPlugin : public InvocationPlugin {
  public:
    IdentifierInvocationPlugin() = default;

    std::optional<Value<>>
    performBinaryOperationAndCast(const Value<>& left,
                                  const Value<>& right,
                                  std::function<Value<>(const Identifier& left, const Identifier& right)> function) const {

        if (!Identifier::isIdentifier(left.getValue()) && !Identifier::isIdentifier(right.getValue())) {
            return std::nullopt;
        }

        if ((Identifier::isIdentifier(left.getValue()) && !Identifier::isIdentifier(right.getValue()))
            || !(Identifier::isIdentifier(left.getValue()) && Identifier::isIdentifier(right.getValue()))) {
            NES_ERROR("Comparison between Identifier and Non-Identifier is not allowed");
            return std::nullopt;
        }

        if (left->getTypeIdentifier()->getTypeInfo() != right->getTypeIdentifier()->getTypeInfo()) {
            NES_ERROR("Comparison between different Identifier Types: {} and {}",
                      left->getTypeIdentifier()->getTypeInfo().name(),
                      right->getTypeIdentifier()->getTypeInfo().name());
            return std::nullopt;
        }

        auto& leftInt = left.getValue().staticCast<Identifier>();
        auto& rightInt = right.getValue().staticCast<Identifier>();
        return function(leftInt, rightInt);
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Identifier& left, const Identifier& right) {
            auto result = left.equals(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Identifier& left, const Identifier& right) {
            auto result = left.lessThan(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Identifier& left, const Identifier& right) {
            auto result = left.greaterThan(right);
            return Value<>(std::move(result));
        });
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<IdentifierInvocationPlugin> intPlugin;
}// namespace NES::Nautilus