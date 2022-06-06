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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#include <Experimental/Interpreter/DataValue/Integer.hpp>
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <execinfo.h>
#include <memory>
#include <stdio.h>
#include <unistd.h>
namespace NES::ExecutionEngine::Experimental::Interpreter {

/*

template<class TFn, typename... Args>
auto MemberFunctionCall(TFn&& f, Args...) {
    std::function<std::remove_reference_t<std::remove_pointer_t<TFn>>> f_ = {std::forward<TFn>(f)};
}

template<class T>
    requires std::is_base_of<Value, T>::value
auto unbox(T value) {
    auto intValue = cast<Integer>(value.value);
    return intValue->value;
}

template<class T>
    requires(std::is_base_of<Value, T>::value == false)
auto unbox(T value) {
    return value;
}

template<typename T, typename... Args>
std::string type_name() {
    if constexpr (!sizeof...(Args)) {
        return std::string(typeid(T).name());
    } else {
        return std::string(typeid(T).name()) + " " + type_name<Args...>();
    }
}


template<class R, class Fn, class... Args>
auto bind2(R Fn::*__pm, Fn* obj, Args... args) {
    using _Traits = std::_Mem_fn_traits<R Fn::*>;
    using _Varargs = typename _Traits::__vararg;
    using __maybe_type = typename _Traits::__maybe_type;
    std::cout << type_name<_Varargs>() << std::endl;
    std::cout << type_name<__maybe_type>() << std::endl;
    auto f_ = std::mem_fn(__pm);
    return f_(obj, (unbox(args))...);
}

template<class Fn, class T, class... Args>
auto bind(Fn T::*__pm, T* obj, Args... args) {
    using _Traits = std::_Mem_fn_traits<Fn T::*>;
    using _Varargs = typename _Traits::__vararg;
    using __maybe_type = typename _Traits::__maybe_type;
    std::cout << type_name<_Varargs>() << std::endl;
    std::cout << type_name<__maybe_type>() << std::endl;
    auto f_ = std::mem_fn(__pm);
    return f_(obj, (unbox(args))...);
}
*/
template<typename Arg>
auto transform(Arg argument) {
    if constexpr (std::is_same<Arg, Value<Integer>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<Boolean>>::value) {
        return argument.value->getValue();
    } else if constexpr (std::is_same<Arg, Value<MemRef>>::value) {
        return (void*) argument.value->getValue();
    }
}

template<typename Arg>
Trace::InputVariant getRefs(Arg argument) {
    return argument.ref;
}

template<typename Arg>
auto transformReturn(Arg argument) {
    if constexpr (std::is_same<Arg, uint64_t>::value) {
        return Value<Integer>(std::make_unique<Integer>(argument));
    }
    if constexpr (std::is_same<Arg, void*>::value) {
        return Value<MemRef>(std::make_unique<MemRef>((int64_t) argument));
    }
}

template<typename R>
auto createDefault() {
    if constexpr (std::is_same<R, uint64_t>::value) {
        return Value<Integer>(std::make_unique<Integer>(0));
    }
    if constexpr (std::is_same<R, void*>::value) {
        return Value<MemRef>(std::make_unique<MemRef>((int64_t) 0));
    }
}

class AbstractFunction {
  public:
    virtual ~AbstractFunction() = default;
    virtual bool operator==(AbstractFunction& other) = 0;
};

template<typename CallbackFunction>
class ProxyFunction : public AbstractFunction {
  public:
    bool operator==(AbstractFunction& other) override {

        auto cast = dynamic_cast<ProxyFunction<CallbackFunction>*>(&other);
        if (cast) {
            return cast->function == this->function && cast->name == this->name;
        }
        return false;
    }

    ProxyFunction(CallbackFunction&& function, std::string& name) : function(function), name(name) {
        void* funptr = reinterpret_cast<void*>(&function);
        backtrace_symbols_fd(&funptr, 1, 1);
    }
    CallbackFunction function;
    std::string name;
};

template<typename CallbackFunction>
std::shared_ptr<AbstractFunction> createProxyFunction(CallbackFunction&& function, std::string name) {
    return std::make_shared<ProxyFunction<CallbackFunction>>(function, name);
}

class ProxyFunctionRegistry {
  public:
    struct ProxyFunctionWrapper {
        std::string name;
        ProxyFunctionWrapper(std::string) { getInstance().registerProxyFunction(this); }
    };
    static ProxyFunctionRegistry& getInstance() {
        static ProxyFunctionRegistry proxyFunctionRegistry = ProxyFunctionRegistry();
        return proxyFunctionRegistry;
    }
    static void registerProxyFunction(ProxyFunctionWrapper*){

    };
};

//static void registerProxyFunction(void* fun, std::string functionName, std::string mangledName);

template<typename CallbackFunction>
void myFunction(CallbackFunction&& function) {
    std::cout << "call function " << &function << "__func__" << __func__ << " ---- " << __PRETTY_FUNCTION__;
}

template<typename R, typename... Args2, typename... Args>
auto FunctionCall(std::string functionName, R (*fnptr)(Args2...), Args... arguments) {

    std::vector<Trace::InputVariant> varRefs = {Trace::FunctionCallTarget(functionName, functionName)};
    for (auto& p : {getRefs(arguments)...}) {
        varRefs.emplace_back(p);
    }

    if constexpr (std::is_void_v<R>) {
        if (!Trace::isInSymbolicExecution()) {
            fnptr(transform(std::forward<Args>(arguments))...);
        }
        auto ctx = Trace::getThreadLocalTraceContext();
        if (ctx != nullptr) {
            auto operation = Trace::Operation(Trace::CALL, varRefs);
            ctx->trace(operation);
        }
    } else {
        if (!Trace::isInSymbolicExecution()) {
            auto res = fnptr(transform(std::forward<Args>(arguments))...);
            auto resultValue = transformReturn(res);
            auto ctx = Trace::getThreadLocalTraceContext();
            if (ctx != nullptr) {

                auto operation = Trace::Operation(Trace::CALL, resultValue.ref, varRefs);
                ctx->trace(operation);
            }
            return resultValue;
        } else {
            auto resultValue = createDefault<R>();
            auto ctx = Trace::getThreadLocalTraceContext();
            auto operation = Trace::Operation(Trace::CALL, resultValue.ref, varRefs);
            ctx->trace(operation);
            return resultValue;
        }
    }
    //}
}
}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
