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
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
namespace NES::Nautilus {

class TextInvocationPlugin : public InvocationPlugin {
  public:
    TextInvocationPlugin() = default;

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        if (left->isType<Text>() && right->isType<Text>()) {
            return left.as<Text>()->equals(right.as<Text>());
        }
        return std::nullopt;
    }

    std::optional<Value<>> ReadArrayIndex(const Value<>& array, Value<UInt32> index) const override {
        if (array->isType<Text>()) {
            return array.as<Text>()->read(index);
        }
        return std::nullopt;
    }

    std::optional<Value<>> WriteArrayIndex(const Value<>& array, Value<UInt32> index, const Value<>& value) const override {
        if (array->isType<Text>() && value->isType<Int8>()) {
            array.as<Text>()->write(index, value.as<Int8>());
            return array;
        }
        return std::nullopt;
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<TextInvocationPlugin> TextInvocationPlugin;
}// namespace NES::Nautilus