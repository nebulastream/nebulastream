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
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Nautilus {

Nautilus::Tracing::ValueRef createNextValueReference(Nautilus::IR::Types::StampPtr&& stamp) {
    if (auto ctx = Nautilus::Tracing::TraceContext::get()) {
        return ctx->createNextRef(stamp);
    }
    return {0, 0, nullptr};
}

std::optional<Value<>> CastToOp(const Value<>& left, const TypeIdentifier* toType) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        if (plugin->IsCastable(left, toType)) {
            auto castedValue = plugin->CastTo(left, toType).value();
            if (auto ctx = Tracing::TraceContext::get()) {
                ctx->traceUnaryOperation(Nautilus::Tracing::OpCode::CAST, left.ref, castedValue.ref);
            }
            return castedValue;
        }
    }
    return std::nullopt;
}
template<>
Value<Any> Value<Any>::castTo(const TypeIdentifier* toStamp) const {
    return CastToOp(*this, toStamp).value();
}

const TraceableType& TraceableType::asTraceableType(const Any& val) { return static_cast<const TraceableType&>(val); }

Value<> evalBinary(
    const Value<>& left,
    const Value<>& right,
    const std::function<
        std::optional<Value<>>(std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right)>& function) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = function(plugin, left, right);
        if (result.has_value()) {
            return result.value();
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation between " << left->toString() << " and "
                                                                                           << right->toString());
}

Value<> evalWithCast(
    const Value<>& left,
    const Value<>& right,
    const std::function<
        std::optional<Value<>>(std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right)>& function) {
    if (left.getValue().getTypeIdentifier() != right.value->getTypeIdentifier()) {
        // try to cast left to right type
        auto castLeft = CastToOp(left, right.value->getTypeIdentifier());
        if (castLeft.has_value()) {
            return evalBinary(castLeft.value(), right, function);
        }
        // try to cast right to left type
        auto castRight = CastToOp(right, left.value->getTypeIdentifier());
        if (castRight.has_value()) {
            return evalBinary(left, castRight.value(), function);
        }
        return evalBinary(left, right, function);
    } else {
        return evalBinary(left, right, function);
    }
}

Value<> AddOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Add(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::ADD, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> SubOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Sub(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::SUB, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> MulOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Mul(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::MUL, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> DivOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Div(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::DIV, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> ModOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Mod(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::MOD, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> BitWiseAndOp(const Value<>& leftExp, const Value<>& rightExp) {
    return evalWithCast(leftExp,
                        rightExp,
                        [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
                            auto result = plugin->BitWiseAnd(left, right);
                            if (result.has_value()) {
                                Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::BITWISE_AND,
                                                                         result.value().ref,
                                                                         left.ref,
                                                                         right.ref);
                            }
                            return result;
                        });
}

Value<> BitWiseOrOp(const Value<>& leftExp, const Value<>& rightExp) {
    return evalWithCast(leftExp,
                        rightExp,
                        [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
                            auto result = plugin->BitWiseOr(left, right);
                            if (result.has_value()) {
                                Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::BITWISE_OR,
                                                                         result.value().ref,
                                                                         left.ref,
                                                                         right.ref);
                            }
                            return result;
                        });
}

Value<> BitWiseXorOp(const Value<>& leftExp, const Value<>& rightExp) {
    return evalWithCast(leftExp,
                        rightExp,
                        [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
                            auto result = plugin->BitWiseXor(left, right);
                            if (result.has_value()) {
                                Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::BITWISE_XOR,
                                                                         result.value().ref,
                                                                         left.ref,
                                                                         right.ref);
                            }
                            return result;
                        });
}

Value<> BitWiseLeftShiftOp(const Value<>& leftExp, const Value<>& rightExp) {
    return evalWithCast(leftExp,
                        rightExp,
                        [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
                            auto result = plugin->BitWiseLeftShift(left, right);
                            if (result.has_value()) {
                                Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::BITWISE_LEFT_SHIFT,
                                                                         result.value().ref,
                                                                         left.ref,
                                                                         right.ref);
                            }
                            return result;
                        });
}

Value<> BitWiseRightShiftOp(const Value<>& leftExp, const Value<>& rightExp) {
    return evalWithCast(leftExp,
                        rightExp,
                        [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
                            auto result = plugin->BitWiseRightShift(left, right);
                            if (result.has_value()) {
                                Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::BITWISE_RIGHT_SHIFT,
                                                                         result.value().ref,
                                                                         left.ref,
                                                                         right.ref);
                            }
                            return result;
                        });
}

Value<> EqualsOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Equals(left, right);
        if (result.has_value() && left.isTracableType() && right.isTracableType()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::EQUALS, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> LessThanOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->LessThan(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::LESS_THAN,
                                                     result.value().ref,
                                                     left.ref,
                                                     right.ref);
        }
        return result;
    });
}

Value<> GreaterThanOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->GreaterThan(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::GREATER_THAN,
                                                     result.value().ref,
                                                     left.ref,
                                                     right.ref);
        }
        return result;
    });
}

Value<> OrOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Or(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::OR, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> AndOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->And(left, right);
        if (result.has_value()) {
            Tracing::TraceUtil::traceBinaryOperation(Nautilus::Tracing::OpCode::AND, result.value().ref, left.ref, right.ref);
        }
        return result;
    });
}

Value<> NegateOp(const Value<>& input) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = plugin->Negate(input);
        if (result.has_value()) {
            Tracing::TraceUtil::traceUnaryOperation(Nautilus::Tracing::OpCode::NEGATE, result->ref, input.ref);
            return result.value();
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation");
}

Value<> ReadArrayIndexOp(const Value<>& input, Value<UInt32>& index) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = plugin->ReadArrayIndex(input, index);
        if (result.has_value()) {
            return result.value();
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation");
}

void WriteArrayIndexOp(const Value<>& input, Value<UInt32>& index, const Value<>& value) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = plugin->WriteArrayIndex(input, index, value);
        if (result.has_value()) {
            return;
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation");
}

template<>
template<>
Value<Text>::Value(const char* s) {
    std::string string(s);
    auto text = TextValue::create(string);
    auto textRef = TypedRef<TextValue>(text);
    this->value = std::make_shared<Text>(textRef);
    this->ref = createNextValueReference(value->getType());
}

template<>
template<typename T, typename>
Value<>::Value(const char* s) {
    std::string string(s);
    auto text = TextValue::create(string);
    auto textRef = TypedRef<TextValue>(text);
    this->value = std::make_shared<Text>(textRef);
    this->ref = createNextValueReference(value->getType());
}

}// namespace NES::Nautilus
