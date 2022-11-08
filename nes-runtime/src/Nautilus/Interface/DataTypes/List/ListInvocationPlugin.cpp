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
#include <Nautilus/Interface/DataTypes/List/List.hpp>
namespace NES::Nautilus {

class ListInvocationPlugin final : public InvocationPlugin {
  public:
    ListInvocationPlugin() = default;

    /**
     * @brief Helper to check if value is a list.
     * @param list
     * @return bool true if value is a list.
     */
    bool isList(const Value<>& list) const { return dynamic_cast<List*>(&list.getValue()) != nullptr; }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        if (isList(left) && isList(right)) {
            return left.as<List>()->equals(right.as<List>());
        }
        return std::nullopt;
    }

    std::optional<Value<>> ReadArrayIndex(const Value<>& array, Value<UInt32> index) const override {
        if (isList(array)) {
            return array.as<List>()->read(index);
        }
        return std::nullopt;
    }

    std::optional<Value<>> WriteArrayIndex(const Value<>& array, Value<UInt32> index, const Value<>& value) const override {
        if (array->isType<List>()) {
            array.as<List>()->write(index, value);
            return array;
        }
        return std::nullopt;
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<ListInvocationPlugin> ListInvocationPlugin;
}// namespace NES::Nautilus