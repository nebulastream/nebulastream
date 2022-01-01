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

#ifndef NES_INCLUDE_API_EXPRESSIONS_LOGICAL_EXPRESSIONS_HPP_
#define NES_INCLUDE_API_EXPRESSIONS_LOGICAL_EXPRESSIONS_HPP_

#include <memory>
#include <type_traits>

namespace NES {

class ExpressionNode;
class ExpressionItem;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

/**
 * @brief Defines common logical operations between expression nodes.
 */
ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp);
ExpressionNodePtr operator!(ExpressionNodePtr exp);
ExpressionNodePtr operator!(ExpressionItem leftExp);

/**
 * @brief Defines common operations on at least one operator which is not of type ExpressionNodePtr but
 * either a constant or an instance of the type ExpressionItem.
 */
/// Utility which converts a constant or an expression item to an ExpressionNodePtr.
template<typename T,
         typename = std::enable_if_t<std::disjunction_v<std::is_same<std::decay_t<T>, ExpressionNodePtr>,
                                                        std::is_same<std::decay_t<T>, ExpressionItem>,
                                                        std::is_constructible<ExpressionItem, std::decay_t<T>>>>>
inline auto toExpressionNodePtr(T&& t) -> ExpressionNodePtr {
    using Arg = std::decay_t<T>;
    if constexpr (std::is_same_v<Arg, ExpressionNodePtr>) {
        // This is actually correct and necessary in C++17 to enable moving in the applicable cases (xval, prval).
        // In C++2a this shouldn't be necessary anymore due to P1825.
        return std::forward<T>(t);
    } else if constexpr (std::is_same_v<Arg, ExpressionItem>) {
        // Guaranteed copy elision
        return t.getExpressionNode();
    }
    // Guaranteed copy elision.
    return ExpressionItem{std::forward<T>(t)}.getExpressionNode();
}

/**
 * @brief True if
 *        a) `T...` are either expression node pointers, expression items or able to construct expression items,
 *            and therefore enable us to create or access an expression node pointer
 *        b) and `T...` are not all already expression node pointers as in that case, there would be no conversion
 *           left to be done.
 *        c) any candidate is either an expression node pointer or an expression item. Otherwise another operator
 *           might be more suitable (e.g. direct integer conversion).
 */
template<typename... T>
static constexpr bool expression_generator_v = std::conjunction_v<
    std::negation<std::conjunction<std::is_same<ExpressionNodePtr, std::decay_t<T>>...>>,
    std::disjunction<std::is_same<ExpressionNodePtr, std::decay_t<T>>..., std::is_same<ExpressionItem, std::decay_t<T>>...>,
    std::disjunction<std::is_constructible<ExpressionItem, T>, std::is_same<ExpressionItem, std::decay_t<T>>>...>;

/**
 * @brief Operator which accepts parameters as long as they can be used to construct an ExpressionItem.
 *        If both the LHS and RHS are ExpressionNodePtrs, this overload is not used.
 *
 * @dev   std::shared_ptr has got a non-explicit constructor for nullptr.
 *        The following construct is used to avoid implicit conversion of `0` to shared_ptr and to convert to an
 *        `ExpressionItem` instead by providing a templated function which accepts all (single) arguments that can be
 *        used to construct an `ExpressionItem`.
 *
 * @tparam LHS the type of the left-hand-side of the operator.
 * @tparam RHS the type of the right-hand-side of the operator.
 *
 * @param lhs  the value of the left-hand-side of the operator.
 * @param rhs  the value of the right-hand-side of the operator.
 *
 * @return ExpressionNodePtr which reflects the operator.
 */
template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator&&(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) && toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator||(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) || toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator==(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) == toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator!=(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) != toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator<=(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) <= toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator>=(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) >= toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator<(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) < toExpressionNodePtr(std::forward<RHS>(rhs));
}

template<typename LHS, typename RHS, typename = std::enable_if_t<expression_generator_v<LHS, RHS>>>
inline auto operator>(LHS&& lhs, RHS&& rhs) -> ExpressionNodePtr {
    return toExpressionNodePtr(std::forward<LHS>(lhs)) > toExpressionNodePtr(std::forward<RHS>(rhs));
}

}// namespace NES

#endif// NES_INCLUDE_API_EXPRESSIONS_LOGICAL_EXPRESSIONS_HPP_
