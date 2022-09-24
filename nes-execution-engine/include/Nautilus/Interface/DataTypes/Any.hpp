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
#include "Experimental/Interpreter/Util/Casting.hpp"
#include "Experimental/NESIR/Operations/Operation.hpp"
#include "Experimental/Utility/CastUtils.hpp"
#include <memory>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class Any;
typedef std::shared_ptr<Any> AnyPtr;

/**
 * @brief Any is the base class of all data types in the interpreter framework.
 */
class Any : public Typed {
  public:
    Any(const TypeIdentifier* identifier);
    template<typename type, typename... Args>
    static std::shared_ptr<type> create(Args&&... args) {
        return std::make_shared<type>(std::forward<Args>(args)...);
    }

    virtual std::shared_ptr<Any> copy() = 0;
    virtual ~Any() = default;

    template<typename Type>
    const Type& staticCast() const {
        return static_cast<const Type&>(*this);
    }

    virtual IR::Types::StampPtr getType() const;
};

class TraceableType : public Any {
  public:
    TraceableType(const TypeIdentifier* identifier) : Any(identifier) {}
    static const TraceableType& asTraceableType(const Any&);
};

template<class X, class Y>
    requires(std::is_same<Any, X>::value)
inline std::shared_ptr<X> cast(const std::shared_ptr<Y>& value) {
    // copy value value
    return value->copy();
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_DATAVALUE_ANY_HPP_
