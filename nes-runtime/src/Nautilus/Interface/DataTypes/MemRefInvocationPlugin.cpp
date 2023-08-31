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
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
namespace NES::Nautilus {

class MemRefInvocationPlugin : public InvocationPlugin {
  public:
    MemRefInvocationPlugin() = default;

    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<MemRef>(left.getValue()) && (isa<Int64>(right.getValue()) || isa<UInt64>(right.getValue()))) {

            auto leftVal = cast<MemRef>(left.getValue());
            if (leftVal->value == nullptr) {
                return Value<>(std::make_shared<MemRef>(nullptr));
            }

            if (auto rightVal = cast_if<Int64>(&right.getValue())) {
                int8_t* result = leftVal->value + rightVal->getValue();
                return Value<>(std::make_shared<MemRef>(result));
            } else if (auto rightVal = cast_if<UInt64>(&right.getValue())) {
                int8_t* result = leftVal->value + rightVal->getValue();
                return Value<>(std::make_shared<MemRef>(result));
            }
        }
        return std::nullopt;
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        if (left->isType<MemRef>() && right->isType<Int32>()) {
            auto result = left.getValue().staticCast<MemRef>().value == nullptr;
            return Value(std::make_shared<Boolean>(result));
        } else if (left->isType<MemRef>() && right->isType<MemRef>()) {
            auto result = left.getValue().staticCast<MemRef>().value == right.getValue().staticCast<MemRef>().value;
            return Value(std::make_shared<Boolean>(result));
        }

        return InvocationPlugin::Equals(left, right);
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<MemRefInvocationPlugin> memRefInvocationPlugin;
}// namespace NES::Nautilus