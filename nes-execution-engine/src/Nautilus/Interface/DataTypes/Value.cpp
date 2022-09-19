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
#include <Experimental/Utility/PluginRegistry.hpp>
#include <Nautilus/Interface/DataValue/Any.hpp>
#include <Nautilus/Interface/DataValue/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataValue/Value.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

std::optional<Value<>> CastToOp(const Value<>& left, const TypeIdentifier* toType) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        if (plugin->IsCastable(left, toType)) {
            auto castedValue = plugin->CastTo(left, toType).value();
            if (auto* ctx = Trace::getThreadLocalTraceContext()) {
                auto operation = Trace::Operation(Trace::CAST, castedValue.ref, {left.ref});
                ctx->trace(operation);
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
    std::function<std::optional<Value<>>(std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right)>
        function) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = function(plugin, left, right);
        if (result.has_value()) {
            return result.value();
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation");
}

Value<> evalWithCast(
    const Value<>& left,
    const Value<>& right,
    std::function<std::optional<Value<>>(std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right)>
        function) {
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
        if (result.has_value() && Trace::getThreadLocalTraceContext()) {
            //if(dynamic_cast<const TraceableType*>(&result.value().getValue()) != nullptr){
            TraceOperation(Trace::OpCode::ADD, left, right, result.value());
            //}
        }
        return result;
    });
}

Value<> SubOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Sub(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::SUB, left, right, result.value());
        }
        return result;
    });
}

Value<> MulOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Mul(left, right);
        if (result.has_value()) {
            //if(static_cast<const TraceableType*>(&result.value().getValue()) != nullptr){
            TraceOperation(Trace::OpCode::MUL, left, right, result.value());
            //}
        }
        return result;
    });
}

Value<> DivOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Div(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::DIV, left, right, result.value());
        }
        return result;
    });
}

Value<> EqualsOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Equals(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::EQUALS, left, right, result.value());
        }
        return result;
    });
}

Value<> LessThanOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->LessThan(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::LESS_THAN, left, right, result.value());
        }
        return result;
    });
}

Value<> GreaterThanOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->LessThan(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::GREATER_THAN, left, right, result.value());
        }
        return result;
    });
}

Value<> OrOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->Or(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::OR, left, right, result.value());
        }
        return result;
    });
}

Value<> AndOp(const Value<>& left, const Value<>& right) {
    return evalWithCast(left, right, [](std::unique_ptr<InvocationPlugin>& plugin, const Value<>& left, const Value<>& right) {
        auto result = plugin->And(left, right);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::AND, left, right, result.value());
        }
        return result;
    });
}

Value<> NegateOp(const Value<>& input) {
    auto& plugins = InvocationPluginRegistry::getPlugins();
    for (auto& plugin : plugins) {
        auto result = plugin->Negate(input);
        if (result.has_value()) {
            TraceOperation(Trace::OpCode::NEGATE, input, result.value());
            return result.value();
        }
    };
    NES_THROW_RUNTIME_ERROR("No plugin registered that can handle this operation");
}

IR::Types::StampPtr Any::getType() const { return IR::Types::StampFactory::createVoidStamp(); }
}// namespace NES::ExecutionEngine::Experimental::Interpreter
