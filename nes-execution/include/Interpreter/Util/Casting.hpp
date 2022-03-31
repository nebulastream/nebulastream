#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_UTIL_CASTING_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_UTIL_CASTING_HPP_
#include <memory>
namespace NES {

class TypeCastable {
  public:
    enum Kind {
        IntegerValue,
        FloatValue,
        TraceValue,
        DoubleValue,
    };
    TypeCastable(Kind k) : kind(k) {}
    Kind getKind() const { return kind; }

  private:
    const Kind kind;
};

template<typename T>
concept GetType = requires(T a) {
    { T::type } -> std::convertible_to<TypeCastable::Kind>;
};

template<class X, class Y>
requires(std::is_base_of<Y, X>::value == false) inline constexpr bool instanceOf(const std::unique_ptr<Y>&) { return false; }

template<GetType X, class Y>
requires(std::is_base_of<Y, X>::value == true) inline bool instanceOf(const std::unique_ptr<Y>& y) { return X::type == y->getKind(); }

template<class X, class Y>
requires(std::is_base_of<Y, X>::value == true) inline std::unique_ptr<X> cast(const std::unique_ptr<Y>& value) {
    // copy value value
    Y* anyVal = value.get();
    X* intValue = static_cast<X*>(anyVal);
    return std::make_unique<X>(*intValue);
    //return std::unique_ptr<X>{static_cast<X*>(value.get())};
}

template<class X, class Y>
requires(std::is_base_of<Y, X>::value == false) inline std::unique_ptr<X> cast(std::unique_ptr<Y>&) { return nullptr; }

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_UTIL_CASTING_HPP_
