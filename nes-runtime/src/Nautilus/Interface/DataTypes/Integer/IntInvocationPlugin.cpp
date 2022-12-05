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
            auto result = left.div(right);
            return Value<>(std::move(result));
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

    template<class Source, class Target>
    Value<> performIntCast(Any& source) const {
        auto& sourceValue = source.staticCast<Source>();
        auto value = sourceValue.getValue();
        return Value<Target>(std::make_unique<Target>(value));
    };

    std::optional<Value<>> CastTo(const Value<>& inputValue, const TypeIdentifier* targetType) const override {
        auto& intValue = static_cast<const Int&>(inputValue.getValue());
        if (targetType->isType<Int16>()) {
            int16_t val = intValue.getRawInt();
            return Value<>(std::make_unique<Int16>(val));
        } else if (targetType->isType<Int32>()) {
            int32_t val = intValue.getRawInt();
            return Value<>(std::make_unique<Int32>(val));
        } else if (targetType->isType<Int64>()) {
            int64_t val = intValue.getRawInt();
            return Value<>(std::make_unique<Int64>(val));
        }

        /*
        auto& leftValue = left.getValue();
        auto inputStamp = TraceableType::asTraceableType(leftValue).getType();

        if (!inputStamp->isInteger() || !stamp->isInteger()) {
            // this method only performs int to int casts.
            return std::nullopt;
        }
        auto sourceStamp = cast<IR::Types::IntegerStamp>(inputStamp);
        auto targetStamp = cast<IR::Types::IntegerStamp>(stamp.get());

        if (sourceStamp->getBitWidth() == IR::Types::IntegerStamp::I8) {
            switch (targetStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I16: return performIntCast<Int8, Int16>(leftValue);
                case IR::Types::IntegerStamp::I32: return performIntCast<Int8, Int32>(leftValue);
                case IR::Types::IntegerStamp::I64: return performIntCast<Int8, Int64>(leftValue);
                default: {
                    NES_THROW_RUNTIME_ERROR("Invalid cast from " << sourceStamp->toString() << " to " << targetStamp->toString());
                }
            }
        } else if (sourceStamp->getBitWidth() == IR::Types::IntegerStamp::I16) {
            switch (targetStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I32: return performIntCast<Int16, Int32>(leftValue);
                case IR::Types::IntegerStamp::I64: return performIntCast<Int16, Int64>(leftValue);
                default: {
                    NES_THROW_RUNTIME_ERROR("Invalid cast from " << sourceStamp->toString() << " to " << targetStamp->toString());
                }
            }
        } else if (sourceStamp->getBitWidth() == IR::Types::IntegerStamp::I32) {
            switch (targetStamp->getBitWidth()) {
                case IR::Types::IntegerStamp::I64: return performIntCast<Int32, Int64>(leftValue);
                default: {
                    NES_THROW_RUNTIME_ERROR("Invalid cast from " << sourceStamp->toString() << " to " << targetStamp->toString());
                }
            }
        }
         */
        return std::nullopt;
    }

    bool IsCastable(const Value<>& value, const TypeIdentifier* targetType) const override {
        if (isa<Int8>(value.getValue())
            && (targetType->isType<Int16>() || targetType->isType<Int32>() || targetType->isType<Int64>())) {
            return true;
        } else if (isa<Int16>(value.getValue()) && (targetType->isType<Int32>() || targetType->isType<Int64>())) {
            return true;
        } else if (isa<Int32>(value.getValue()) && (targetType->isType<Int64>())) {
            return true;
        }
        return false;
    }
};

[[maybe_unused]] static InvocationPluginRegistry::Add<IntInvocationPlugin> intPlugin;
}// namespace NES::Nautilus