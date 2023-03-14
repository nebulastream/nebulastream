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
#include <Nautilus/IR/Types/FloatStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Double.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
namespace NES::Nautilus {

class FloatInvocationPlugin : public InvocationPlugin {
  public:
    FloatInvocationPlugin() = default;

    std::optional<Value<>>
    performBinaryOperationAndCast(const Value<>& left,
                                  const Value<>& right,
                                  std::function<Value<>(const Any& left, const Any& right)> function) const {
        auto& leftValue = left.getValue();
        auto& rightValue = right.getValue();
        if ((isa<Float>(leftValue) && isa<Float>(rightValue)) || (isa<Double>(leftValue) && isa<Double>(rightValue))) {
            return function(leftValue, rightValue);
        }
        NES_TRACE("FloatInvocationPlugin is not suitable");
        return std::nullopt;
    }

    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().add(right.staticCast<Float>());
                auto resultValue = Value<>(std::move(result));
                return resultValue;
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().add(right.staticCast<Double>());
                auto resultValue = Value<>(std::move(result));
                return resultValue;
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> Sub(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().sub(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().sub(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> Mul(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().mul(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().mul(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> Div(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().div(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().div(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().equals(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().equals(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().lessThan(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().lessThan(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Any& left, const Any& right) {
            if (isa<Float>(left) && isa<Float>(right)) {
                auto result = left.staticCast<Float>().greaterThan(right.staticCast<Float>());
                return Value<>(std::move(result));
            } else if (isa<Double>(left) && isa<Double>(right)) {
                auto result = left.staticCast<Double>().greaterThan(right.staticCast<Double>());
                return Value<>(std::move(result));
            }
            NES_THROW_RUNTIME_ERROR("");
        });
    }

    template<class T>
    std::optional<Value<>> castIntToValue(const Value<>& left, const TypeIdentifier* toType) const {
        auto typedValue = left.as<T>();
        if (toType->isType<Float>()) {
            auto value = typedValue.getValue().getRawInt();
            return Value<>(std::make_unique<Float>(value));
        } else {
            auto value = typedValue.getValue().getRawInt();
            return Value<>(std::make_unique<Double>(value));
        }
    }

    std::optional<Value<>> CastTo(const Value<>& left, const TypeIdentifier* toType) const override {
        // this method only performs float to double casts.
        if (isa<Float>(left.value) && toType->isType<Double>()) {
            // cast float to double
            auto& leftValue = left.getValue();
            auto& sourceValue = leftValue.staticCast<Float>();
            auto value = sourceValue.getValue();
            return Value<>(std::make_unique<Double>(value));
        }

        if (isa<UInt8>(left.value)) {
            return castIntToValue<UInt8>(left, toType);
        } else if (isa<UInt16>(left.value)) {
            return castIntToValue<UInt16>(left, toType);
        } else if (isa<UInt16>(left.value)) {
            return castIntToValue<UInt16>(left, toType);
        } else if (isa<UInt32>(left.value)) {
            return castIntToValue<UInt32>(left, toType);
        } else if (isa<UInt64>(left.value)) {
            return castIntToValue<UInt64>(left, toType);
        } else if (isa<Int8>(left.value)) {
            return castIntToValue<Int8>(left, toType);
        } else if (isa<Int16>(left.value)) {
            return castIntToValue<Int16>(left, toType);
        } else if (isa<Int16>(left.value)) {
            return castIntToValue<Int16>(left, toType);
        } else if (isa<Int32>(left.value)) {
            return castIntToValue<Int32>(left, toType);
        } else if (isa<Int64>(left.value)) {
            return castIntToValue<Int64>(left, toType);
        }
        return std::nullopt;
    }

    bool IsCastable(const Value<>& value, const TypeIdentifier* toType) const override {
        // cast from float to double
        if (isa<Float>(value.getValue()) && toType->isType<Double>()) {
            return true;
        }
        // cast from int to float or double
        return (isa<UInt8>(value.getValue()) || isa<UInt16>(value.getValue()) || isa<UInt32>(value.getValue())
                || isa<UInt64>(value.getValue()) || isa<Int8>(value.getValue()) || isa<Int16>(value.getValue())
                || isa<Int32>(value.getValue()) || isa<Int64>(value.getValue()))
            && (toType->isType<Float>() || toType->isType<Double>());
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<FloatInvocationPlugin> floatPlugin;
}// namespace NES::Nautilus