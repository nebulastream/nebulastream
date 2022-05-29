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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
#include <Experimental/Interpreter/Util/Casting.hpp>
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

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

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
