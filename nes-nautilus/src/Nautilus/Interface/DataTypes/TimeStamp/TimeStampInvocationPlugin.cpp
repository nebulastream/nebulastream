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
#include <Nautilus/Interface/DataTypes/TimeStamp/TimeStamp.hpp>
#include <functional>
namespace NES::Nautilus {

class TimeStampInvocationPlugin : public InvocationPlugin {
  public:
    TimeStampInvocationPlugin() = default;

    std::optional<Value<>>
    performBinaryOperationAndCast(const Value<>& left,
                                  const Value<>& right,
                                  std::function<Value<>(const TimeStamp& left, const TimeStamp& right)> function) const {
        auto& leftValue = left.getValue();
        auto& rightValue = right.getValue();
        if (isa<TimeStamp>(leftValue) && isa<TimeStamp>(rightValue)) {
            auto& leftTs = leftValue.staticCast<TimeStamp>();
            auto& rightTs = rightValue.staticCast<TimeStamp>();
            return function(leftTs, rightTs);
        }
        return std::nullopt;
    }

    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        if (isa<TimeStamp>(left.value) && isa<TimeStamp>(right.value)) {
            auto& ct1 = left.getValue().staticCast<TimeStamp>();
            auto& ct2 = right.getValue().staticCast<TimeStamp>();
            return Value(ct1.add(ct2));
        } else {
            return std::nullopt;
        }
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const TimeStamp& left, const TimeStamp& right) {
            auto result = left.equals(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const TimeStamp& left, const TimeStamp& right) {
            auto result = left.lessThan(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const TimeStamp& left, const TimeStamp& right) {
            auto result = left.greaterThan(right);
            return Value<>(std::move(result));
        });
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<TimeStampInvocationPlugin> cPlugin;

}// namespace NES::Nautilus