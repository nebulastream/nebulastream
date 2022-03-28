#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
#include <memory>
namespace NES {

class Value;
typedef std::unique_ptr<Value> ValueUPtr;

class Value {
  public:
    virtual ValueUPtr equals(ValueUPtr) const;

    virtual ValueUPtr sub(ValueUPtr) const;
    virtual ValueUPtr div(ValueUPtr) const;
    virtual ValueUPtr mul(ValueUPtr) const;
    virtual ValueUPtr le(ValueUPtr) const;
    virtual ValueUPtr lt(ValueUPtr) const;
    virtual ValueUPtr ge(ValueUPtr) const;
    virtual ValueUPtr gt(ValueUPtr) const;
};

template <class To, class From>
struct cast_retty_impl<To, std::unique_ptr<From>> {
  private:
    using PointerType = typename cast_retty_impl<To, From *>::ret_type;
    using ResultType = std::remove_pointer_t<PointerType>;

  public:
    using ret_type = std::unique_ptr<ResultType>;
};

template <class X, class Y>
inline typename cast_retty<X, std::unique_ptr<Y>>::ret_type
cast(std::unique_ptr<Y> &&Val) {
    assert(isa<X>(Val.get()) && "cast<Ty>() argument of incompatible type!");
    using ret_type = typename cast_retty<X, std::unique_ptr<Y>>::ret_type;
    return ret_type(
        cast_convert_val<X, Y *, typename simplify_type<Y *>::SimpleType>::doit(
            Val.release()));
}


class Integer : public Value {
  public:
    Integer(uint64_t value) : value(value){};
    std::unique_ptr<Integer> add(std::unique_ptr<Integer> otherValue) const {
        return std::make_unique<Integer>(value + otherValue->value);
    };

    uint64_t value;
};





ValueUPtr operator+(ValueUPtr& leftExp, ValueUPtr& rightExp) {
    // if left == right kind
   auto integer =  std::make_unique<Integer>(leftExp.release());

}

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_VALUE_HPP_
