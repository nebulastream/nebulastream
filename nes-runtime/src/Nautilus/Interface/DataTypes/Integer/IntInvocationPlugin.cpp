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
#include <functional>
namespace NES::Nautilus {

class IntInvocationPlugin : public InvocationPlugin {
  public:
    IntInvocationPlugin() = default;

    std::optional<Value<>>
    performBinaryOperationAndCast(const Value<>& left,
                                  const Value<>& right,
                                  std::function<Value<>(const Int& left, const Int& right)> function) const {
        auto& leftValue = left.getValue();
        auto& rightValue = right.getValue();
        if (Int::isInteger(leftValue) && Int::isInteger(rightValue)) {
            if (leftValue.getTypeIdentifier() != rightValue.getTypeIdentifier()) {
                NES_THROW_RUNTIME_ERROR("Implicit casts between different are not supported.");
            }

            auto& leftInt = leftValue.staticCast<Int>();
            auto& rightInt = rightValue.staticCast<Int>();
            return function(leftInt, rightInt);
        }
        return std::nullopt;
    }

    std::optional<Value<>> Add(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.add(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> Sub(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.sub(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> Mul(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.mul(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> Div(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            if (Tracing::TraceUtil::inTracer()) {
                // FIXME avoid division in tracing. For now we just substitute with an add.
                auto result = left.add(right);
                return Value<>(std::move(result));
            } else {
                auto result = left.div(right);
                return Value<>(std::move(result));
            }
        });
    }

    std::optional<Value<>> Mod(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            if (Tracing::TraceUtil::inTracer()) {
                // FIXME avoid modulo in tracing. For now we just substitute with an add.
                auto result = left.add(right);
                return Value<>(std::move(result));
            } else {
                auto result = left.mod(right);
                return Value<>(std::move(result));
            }
        });
    }

    std::optional<Value<>> Equals(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.equals(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> LessThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.lessThan(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> GreaterThan(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.greaterThan(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> BitWiseAnd(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.bitWiseAnd(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> BitWiseOr(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.bitWiseOr(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> BitWiseXor(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.bitWiseXor(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> BitWiseLeftShift(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.bitWiseLeftShift(right);
            return Value<>(std::move(result));
        });
    }

    std::optional<Value<>> BitWiseRightShift(const Value<>& left, const Value<>& right) const override {
        return performBinaryOperationAndCast(left, right, [](const Int& left, const Int& right) {
            auto result = left.bitWiseRightShift(right);
            return Value<>(std::move(result));
        });
    }

    template<class Source, class Target>
    Value<> performIntCast(Any& source) const {
        auto& sourceValue = source.staticCast<Source>();
        auto value = sourceValue.getValue();
        return Value<Target>(std::make_unique<Target>(value));
    };

    std::optional<Value<>> CastTo(const Value<>& inputValue, const TypeIdentifier* targetType) const override {
        auto& intValue = inputValue->staticCast<Int>();
        if (targetType->isType<Int16>()) {
            auto val = (int16_t) intValue.getRawInt();
            return Value<>(std::make_unique<Int16>(val));
        } else if (targetType->isType<Int32>()) {
            auto val = (int32_t) intValue.getRawInt();
            return Value<>(std::make_unique<Int32>(val));
        } else if (targetType->isType<Int64>()) {
            auto val = (int64_t) intValue.getRawInt();
            return Value<>(std::make_unique<Int64>(val));
        } else if (targetType->isType<UInt8>()) {
            auto val = (uint8_t) intValue.getRawInt();
            return Value<>(std::make_unique<UInt8>(val));
        } else if (targetType->isType<UInt16>()) {
            auto val = (uint16_t) intValue.getRawInt();
            return Value<>(std::make_unique<UInt16>(val));
        } else if (targetType->isType<UInt32>()) {
            auto val = (uint32_t) intValue.getRawInt();
            return Value<>(std::make_unique<UInt32>(val));
        } else if (targetType->isType<UInt64>()) {
            auto val = (uint64_t) intValue.getRawInt();
            return Value<>(std::make_unique<UInt64>(val));
        }
        return std::nullopt;
    }

    bool IsCastable(const Value<>& value, const TypeIdentifier* targetType) const override {
        // signed conversions
        if (isa<Int8>(value.getValue())
            && (targetType->isType<Int16>() || targetType->isType<Int32>() || targetType->isType<Int64>()
                || targetType->isType<UInt8>() || targetType->isType<UInt16>() || targetType->isType<UInt32>()
                || targetType->isType<UInt64>())) {
            // int8 can be casted to Int16-Int64 and UInt8-UInt64
            return true;
        } else if (isa<Int16>(value.getValue())
                   && (targetType->isType<Int32>() || targetType->isType<Int64>() || targetType->isType<UInt16>()
                       || targetType->isType<UInt32>() || targetType->isType<UInt64>())) {
            // int16 can be casted to Int32-Int64 and UInt16-UInt64
            return true;
        } else if (isa<Int32>(value.getValue())
                   && (targetType->isType<Int64>() || targetType->isType<UInt32>() || targetType->isType<UInt64>())) {
            // int32 can be casted to Int64 and UInt32-UInt64
            return true;
        } else if (isa<Int64>(value.getValue()) && targetType->isType<UInt64>()) {
            // int32 can be casted to Int64 and UInt32-UInt64
            return true;
        }
        // unsigned conversions
        if (isa<UInt8>(value.getValue())
            && (targetType->isType<Int16>() || targetType->isType<Int32>() || targetType->isType<Int64>()
                || targetType->isType<UInt16>() || targetType->isType<UInt32>() || targetType->isType<UInt64>())) {
            // uint8 can be casted to Int16-Int64 and UInt16-UInt64
            return true;
        } else if (isa<UInt16>(value.getValue())
                   && (targetType->isType<Int32>() || targetType->isType<Int64>() || targetType->isType<UInt32>()
                       || targetType->isType<UInt64>())) {
            // uint16 can be casted to Int32-Int64 and UInt32-UInt64
            return true;
        } else if (isa<UInt32>(value.getValue()) && (targetType->isType<Int64>() || targetType->isType<UInt64>())) {
            // uint32 can be casted to Int64 and UInt64
            return true;
        }
        return false;
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<IntInvocationPlugin> intPlugin;
}// namespace NES::Nautilus