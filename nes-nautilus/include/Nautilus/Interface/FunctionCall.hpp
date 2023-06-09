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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FUNCTIONCALL_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FUNCTIONCALL_HPP_
#include <Nautilus/IR/Types/StampFactory.hpp>
#include <Nautilus/Interface/DataTypes/BaseTypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Tracing/Trace/TraceOperation.hpp>
#include <Nautilus/Tracing/TraceUtil.hpp>
#include <Util/StdInt.hpp>
#include <cstdio>
#include <memory>
#include <unistd.h>
namespace NES::Nautilus {

template<class T>
struct dependent_false : std::false_type {};

template<typename Arg>
auto transform(Arg argument) {
    if constexpr (std::is_same<Arg, Value<Int8>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Int16>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Int32>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Int64>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<UInt8>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<UInt16>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<UInt32>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<UInt64>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Float>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Double>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Boolean>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<MemRef>>::value) {
        return (void*) argument.value->getValue();
    } else if constexpr (std::is_base_of<BaseTypedRef, Arg>::value) {
        return argument.get();
    } else {
        static_assert(dependent_false<Arg>::value);
    }
}

template<typename Arg>
Nautilus::Tracing::InputVariant getRefs(Arg& argument) {
    if constexpr (std::is_base_of<BaseTypedRef, Arg>::value) {
        return argument.value->ref;
    } else {
        return argument.ref;
    }
}

template<typename Arg>
auto transformReturnValues(Arg argument) {
    if constexpr (std::is_same<Arg, int8_t>::value) {
        return Value<Int8>(std::make_unique<Int8>(argument));
    } else if constexpr (std::is_same<Arg, int16_t>::value) {
        return Value<Int16>(std::make_unique<Int16>(argument));
    } else if constexpr (std::is_same<Arg, int32_t>::value) {
        return Value<Int32>(std::make_unique<Int32>(argument));
    } else if constexpr (std::is_same<Arg, int64_t>::value) {
        return Value<Int64>(std::make_unique<Int64>(argument));
    } else if constexpr (std::is_same<Arg, uint8_t>::value) {
        return Value<UInt8>(std::make_unique<UInt8>(argument));
    } else if constexpr (std::is_same<Arg, uint16_t>::value) {
        return Value<UInt16>(std::make_unique<UInt16>(argument));
    } else if constexpr (std::is_same<Arg, uint32_t>::value) {
        return Value<UInt32>(std::make_unique<UInt32>(argument));
    } else if constexpr (std::is_same<Arg, uint64_t>::value) {
        return Value<UInt64>(std::make_unique<UInt64>(argument));
    } else if constexpr (std::is_same<Arg, void*>::value) {
        return Value<MemRef>(std::make_unique<MemRef>((int8_t*) argument));
    } else if constexpr (std::is_same<Arg, uint8_t*>::value) {
        return Value<MemRef>(std::make_unique<MemRef>((int64_t) argument));
    } else if constexpr (std::is_same<Arg, bool>::value) {
        return Value<Boolean>(std::make_unique<Boolean>((bool) argument));
    } else if constexpr (std::is_same<Arg, float>::value) {
        return Value<Float>(std::make_unique<Float>(argument));
    } else if constexpr (std::is_same<Arg, double>::value) {
        return Value<Double>(std::make_unique<Double>(argument));
    } else {
        static_assert(dependent_false<Arg>::value);
    }
}

class TextValue;
template<typename T>
    requires std::is_same_v<TextValue*, T>
auto createDefault();

class BaseListValue;
template<typename T>
    requires std::is_base_of<BaseListValue, typename std::remove_pointer<T>::type>::value
auto createDefault();

template<typename R>
    requires std::is_fundamental_v<R> || std::is_same_v<void*, R>
auto createDefault() {
    if constexpr (std::is_same<R, int8_t>::value) {
        return Value<Int8>(std::make_unique<Int8>(0));
    } else if constexpr (std::is_same<R, int16_t>::value) {
        return Value<Int16>(std::make_unique<Int16>(0));
    } else if constexpr (std::is_same<R, int32_t>::value) {
        return Value<Int32>(std::make_unique<Int32>(0));
    } else if constexpr (std::is_same<R, int64_t>::value) {
        return Value<Int64>(std::make_unique<Int64>(0));
    } else if constexpr (std::is_same<R, uint8_t>::value) {
        return Value<UInt8>(std::make_unique<UInt8>(0));
    } else if constexpr (std::is_same<R, uint16_t>::value) {
        return Value<UInt16>(std::make_unique<UInt16>(0));
    } else if constexpr (std::is_same<R, uint32_t>::value) {
        return Value<UInt32>(std::make_unique<UInt32>(0));
    } else if constexpr (std::is_same<R, uint64_t>::value) {
        return Value<UInt64>(std::make_unique<UInt64>(0));
    } else if constexpr (std::is_same<R, float>::value) {
        return Value<Float>(std::make_unique<Float>(0.0f));
    } else if constexpr (std::is_same<R, double>::value) {
        return Value<Double>(std::make_unique<Double>(0.0));
    } else if constexpr (std::is_same<R, bool>::value) {
        return Value<Boolean>(std::make_unique<Boolean>(false));
    } else if constexpr (std::is_same<R, void*>::value) {
        return Value<MemRef>(std::make_unique<MemRef>(nullptr));
    } else {
        static_assert(dependent_false<R>::value);
    }
}

void traceFunctionCall(Nautilus::Tracing::ValueRef& resultRef, const std::vector<Nautilus::Tracing::InputVariant>& arguments);
void traceVoidFunctionCall(const std::vector<Nautilus::Tracing::InputVariant>& arguments);

template<typename... ValueArguments>
auto getArgumentReferences(std::string functionName, void* fnptr, ValueArguments... arguments) {
    std::vector<Nautilus::Tracing::InputVariant> functionArgumentReferences = {
        Nautilus::Tracing::FunctionCallTarget(functionName, fnptr)};
    if constexpr (sizeof...(ValueArguments) > 0) {
        for (const Nautilus::Tracing::InputVariant& p : {getRefs(arguments)...}) {
            functionArgumentReferences.emplace_back(p);
        }
    }
    return functionArgumentReferences;
}

template<typename R, typename... FunctionArguments, typename... ValueArguments>
auto FunctionCall(std::string functionName, R (*fnptr)(FunctionArguments...), ValueArguments... arguments) {
    if constexpr (std::is_void_v<R>) {
        if (Tracing::TraceUtil::inInterpreter()) {
            fnptr(transform(std::forward<ValueArguments>(arguments))...);
        } else {
            auto functionArgumentReferences = getArgumentReferences(functionName, (void*) fnptr, arguments...);
            traceVoidFunctionCall(functionArgumentReferences);
        }
    } else {
        if (Tracing::TraceUtil::inInterpreter()) {
            auto functionResult = fnptr(transform(std::forward<ValueArguments>(arguments))...);
            return transformReturnValues(functionResult);
        } else {
            auto resultValue = createDefault<R>();
            auto functionArgumentReferences = getArgumentReferences(functionName, (void*) fnptr, arguments...);
            traceFunctionCall(resultValue.ref, functionArgumentReferences);
            return resultValue;
        }
    }
}

}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_FUNCTIONCALL_HPP_
