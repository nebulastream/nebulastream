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
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
namespace NES::Nautilus {

class BooleanInvocationPlugin : public InvocationPlugin {
  public:
    BooleanInvocationPlugin() = default;

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        if (left->isType<Boolean>() && right->isType<Boolean>()) {
            auto& leftVal = left.getValue().staticCast<Boolean>();
            auto& rightVal = right.getValue().staticCast<Boolean>();
            return Value(std::make_shared<Boolean>(leftVal == rightVal));
        }
        return std::nullopt;
    }

    std::optional<Value<>> Negate(const Value<>& left) const override {
        auto& val = left.getValue().staticCast<Boolean>();
        return Value(std::make_shared<Boolean>(!val));
    }

    std::optional<Value<>> And(const Value<>& left, const Value<>& right) const override {
        auto& leftVal = left.getValue().staticCast<Boolean>();
        auto& rightVal = right.getValue().staticCast<Boolean>();
        return Value(std::make_shared<Boolean>(leftVal && rightVal));
    }

    std::optional<Value<>> Or(const Value<>& left, const Value<>& right) const override {
        auto& leftVal = left.getValue().staticCast<Boolean>();
        auto& rightVal = right.getValue().staticCast<Boolean>();
        return Value(std::make_shared<Boolean>(leftVal || rightVal));
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<BooleanInvocationPlugin> booleanInvocationPlugin;
}// namespace NES::Nautilus