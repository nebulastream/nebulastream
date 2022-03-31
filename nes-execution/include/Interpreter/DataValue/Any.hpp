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

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
