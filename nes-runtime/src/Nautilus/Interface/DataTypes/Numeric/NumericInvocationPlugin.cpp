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
#include <Nautilus/Interface/DataTypes/Numeric/Numeric.hpp>
namespace NES::Nautilus {

class NumericInvocationPlugin : public InvocationPlugin {
  public:
    NumericInvocationPlugin() = default;

    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            auto righ = right.as<Numeric>();
            return left.as<Numeric>()->add(righ);
        }
        return InvocationPlugin::Add(left, right);
    }
    std::optional<Value<>> Sub(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            auto righ = right.as<Numeric>();
            return left.as<Numeric>()->sub(righ);
        }
        return InvocationPlugin::Sub(left, right);
    }
    std::optional<Value<>> Mul(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            auto righ = right.as<Numeric>();
            return left.as<Numeric>()->mul(righ);
        }
        return InvocationPlugin::Mul(left, right);
    }
    std::optional<Value<>> Div(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            auto righ = right.as<Numeric>();
            return left.as<Numeric>()->div(righ);
        }
        return InvocationPlugin::Div(left, right);
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            return left.as<Numeric>()->equals(right.as<Numeric>());
        }
        return InvocationPlugin::Equals(left, right);
    }

    std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            return left.as<Numeric>()->lessThan(right.as<Numeric>());
        }
        return InvocationPlugin::LessThan(left, right);
    }
    std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const override {
        if(left->isType<Numeric>() && right->isType<Numeric>()){
            return left.as<Numeric>()->greaterThan(right.as<Numeric>());
        }
        return InvocationPlugin::GreaterThan(left, right);
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<NumericInvocationPlugin> NumericInvocationPlugin;
}// namespace NES::Nautilus