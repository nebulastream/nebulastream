#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
#include <Interpreter/DataValue/Integer.hpp>
#include <memory>
namespace NES::Interpreter {


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

template<typename R, typename... Args>
std::conditional_t<std::is_void_v<R>, void, R> FunctionCall(R (*fnptr)(Args...), Args... arguments) {
    if constexpr (std::is_void_v<R>) {
        fnptr(std::forward<Args...>());
        //((c->setArg(index++, arguments)), ...);
    } else {
        return fnptr(arguments...);
    }
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_FUNCTIONCALL_HPP_
