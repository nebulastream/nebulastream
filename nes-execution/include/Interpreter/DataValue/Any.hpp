#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
#include <Interpreter/Util/Casting.hpp>
#include <memory>

namespace NES::Interpreter {

class Any;
typedef std::shared_ptr<Any> AnyPtr;

class Any : public TypeCastable {
  public:
    template<typename type, typename... Args>
    static std::unique_ptr<type> create(Args&&... args) {
        return std::make_unique<type>(std::forward<Args>(args)...);
    }

    virtual std::unique_ptr<Any> copy() = 0;
    virtual ~Any() = default;

    Any(Kind k) : TypeCastable(k){};
};

template<class X, class Y>
    requires(std::is_same<Any, X>::value)
inline std::unique_ptr<X> cast(const std::unique_ptr<Y>& value) {
    // copy value value
    return value->copy();
}

template<class X, class Y>
    requires(std::is_base_of<Y, X>::value == true && std::is_same<Any, X>::value == false)
inline std::unique_ptr<X> cast(const std::unique_ptr<Y>& value) {
    // copy value value
    Y* anyVal = value.get();
    X* intValue = static_cast<X*>(anyVal);
    return std::make_unique<X>(*intValue);
    //return std::unique_ptr<X>{static_cast<X*>(value.get())};
}

template<class X, class Y>
    requires(std::is_base_of<Y, X>::value == false && std::is_same<Any, X>::value == false)
inline std::unique_ptr<X> cast(const std::unique_ptr<Y>& ) {
    // copy value value
    return nullptr;
    //return std::unique_ptr<X>{static_cast<X*>(value.get())};
}

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
