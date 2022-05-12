#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#include <Interpreter/DataValue/Integer.hpp>
#include <execinfo.h>
#include <memory>
#include <stdio.h>
#include <unistd.h>
namespace NES::Interpreter {

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
    ProxyFunctionRegistry& getInstance() {
        static ProxyFunctionRegistry proxyFunctionRegistry = ProxyFunctionRegistry();
        return proxyFunctionRegistry;
    }
};

template<typename CallbackFunction>
void myFunction(CallbackFunction&& function) {
    std::cout << "call function " << &function << "__func__" << __func__ << " ---- " << __PRETTY_FUNCTION__;
}

template<typename R, typename... Args2, typename... Args>
std::conditional_t<std::is_void_v<R>, void, R> FunctionCall(R (*fnptr)(Args2...), Args... arguments) {
    if constexpr (std::is_void_v<R>) {
        fnptr(std::forward<Args...>());
        //((c->setArg(index++, arguments)), ...);
    } else {
        std::cout << "call function " << &fnptr << "__func__" << __func__ << " ---- " << __PRETTY_FUNCTION__;

        printf("%p\n", fnptr);
        return fnptr(transform(std::forward<Args>(arguments))...);
    }
}
void foo(void) { printf("foo\n"); }
}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
