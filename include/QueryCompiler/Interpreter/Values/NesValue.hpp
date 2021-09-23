/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESVALUE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESVALUE_HPP_
#include <QueryCompiler/Interpreter/ForwardDeclaration.hpp>
#include <memory>
#include <type_traits>
#include <utility>
namespace NES::QueryCompilation {

class ValueHolder {

  public:
    int printValue(int a, int b);
};

template<typename R, typename T, typename A1,  typename A2>
R call2(R (T::* v)(A1, A2)){
    static_cast<R(T::*)(A1 attribute, A2 attribute2)>(v);
}

template<typename T, typename R>
R call(T&& t){
    return t();
};

class NesValue : public std::enable_shared_from_this<NesValue> {
  public:
    virtual operator bool() { return false; };
    virtual NesValuePtr equals(NesValuePtr) const;
    virtual NesValuePtr add(NesValuePtr) const;
    virtual NesValuePtr sub(NesValuePtr) const;
    virtual NesValuePtr div(NesValuePtr) const;
    virtual NesValuePtr mul(NesValuePtr) const;
    virtual NesValuePtr le(NesValuePtr) const;
    virtual NesValuePtr lt(NesValuePtr) const;
    virtual NesValuePtr ge(NesValuePtr) const;
    virtual NesValuePtr gt(NesValuePtr) const;
    virtual void write(NesMemoryAddressPtr address) const;
    virtual ~NesValue() = default;
};

NesValuePtr createNesInt(int32_t v);
NesValuePtr createNesLong(int64_t v);

/**
 * @brief True if
 *        a) `T...` are either expression node pointers, expression items or able to construct expression items,
 *            and therefore enable us to create or access an expression node pointer
 *        b) and `T...` are not all already expression node pointers as in that case, there would be no conversion
 *           left to be done.
 *        c) any candidate is either an expression node pointer or an expression item. Otherwise another operator
 *           might be more suitable (e.g. direct integer conversion).
 */
template<typename T>
requires std::is_convertible_v<T, NesValuePtr> || std::is_arithmetic_v<std::decay_t<T>>
inline auto toNesValue(T&& t) -> NesValuePtr {
    using Arg = std::decay_t<T>;
    if constexpr (std::is_convertible_v<Arg, NesValuePtr>) {
        return std::forward<T>(t);
    } else if constexpr (std::is_convertible_v<Arg, int32_t>) {
        return createNesInt(t);
    } else if constexpr (std::is_convertible_v<Arg, int64_t>) {
        return createNesLong(t);
    } else if constexpr (std::is_convertible_v<Arg, uint64_t>) {
        return createNesLong(10);
    }
}

template<typename LHS, typename RHS>
static constexpr bool
is_binary_constructable_value = (std::is_convertible_v<LHS, NesValuePtr> && std::is_convertible_v<RHS, NesValuePtr>)
    || (std::is_convertible_v<LHS, NesValuePtr> && std::is_arithmetic_v<std::decay_t<RHS>>)
    || (std::is_convertible_v<RHS, NesValuePtr> && std::is_arithmetic_v<std::decay_t<LHS>>);

template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator<(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->lt(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator==(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->equals(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator>(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->gt(toNesValue(std::forward<RHS>(rhs)));
}
template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator>=(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->ge(toNesValue(std::forward<RHS>(rhs)));
}
template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator<=(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->le(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires std::is_convertible_v<LHS, NesValuePtr>
inline auto operator+(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->add(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator-(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->sub(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires is_binary_constructable_value<LHS, RHS>
inline auto operator*(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->mul(toNesValue(std::forward<RHS>(rhs)));
}

template<typename LHS, typename RHS>
requires std::is_convertible_v<LHS, NesValuePtr>
inline auto operator/(LHS&& lhs, RHS&& rhs) -> NesValuePtr {
    return toNesValue(std::forward<LHS>(lhs))->div(toNesValue(std::forward<RHS>(rhs)));
}

}// namespace NES::QueryCompilation

#endif//NES_INCLUDE_QUERYCOMPILER_INTERPRETER_DATATYPES_NESVALUE_HPP_
