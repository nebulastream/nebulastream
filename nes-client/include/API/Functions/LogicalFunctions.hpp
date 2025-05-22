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

#pragma once

#include <memory>
#include <type_traits>
#include <API/Functions/Functions.hpp>
#include <Functions/NodeFunction.hpp>

namespace NES
{

/// Defines common logical operations between function nodes.
std::shared_ptr<NodeFunction>
operator&&(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator||(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator==(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator!=(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator<=(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator>=(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator<(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction>
operator>(const std::shared_ptr<NodeFunction>& functionLeft, const std::shared_ptr<NodeFunction>& functionRight);
std::shared_ptr<NodeFunction> operator!(const std::shared_ptr<NodeFunction>& exp);
std::shared_ptr<NodeFunction> operator!(const FunctionItem& exp);

/**
 * @brief Defines common operations on at least one operator which is not of type std::shared_ptr<NodeFunction> but
 * either a constant or an instance of the type FunctionItem.
 */
///NodeFunction Utility which converts a constant or an function item to an Ptr.
template <
    typename T,
    typename = std::enable_if_t<std::disjunction_v<
        std::is_same<std::decay_t<T>, std::shared_ptr<NodeFunction>>,
        std::is_same<std::decay_t<T>, FunctionItem>,
        std::is_constructible<FunctionItem, std::decay_t<T>>>>>
std::shared_ptr<NodeFunction> toNodeFunctionPtr(T&& item)
{
    using Arg = std::decay_t<T>;
    if constexpr (std::is_same_v<Arg, std::shared_ptr<NodeFunction>>)
    {
        /// This is actually correct and necessary in C++17 to enable moving in the applicable cases (xval, prval).
        /// In C++2a this shouldn't be necessary anymore due to P1825.
        return std::forward<T>(item);
    }
    else if constexpr (std::is_same_v<Arg, FunctionItem>)
    {
        /// Guaranteed copy elision
        return item.getNodeFunction();
    }
    /// Guaranteed copy elision.
    return FunctionItem{std::forward<T>(item)}.getNodeFunction();
}

/**
 * @brief True if
 *        a) `T...` are either function node pointers, function items or able to construct function items,
 *            and therefore enable us to create or access an function node pointer
 *        b) and `T...` are not all already function node pointers as in that case, there would be no conversion
 *           left to be done.
 *        c) any candidate is either an function node pointer or an function item. Otherwise another operator
 *           might be more suitable (e.g. direct integer conversion).
 */
template <typename... T>
static constexpr bool function_generator_v = std::conjunction_v<
    std::negation<std::conjunction<std::is_same<std::shared_ptr<NodeFunction>, std::decay_t<T>>...>>,
    std::disjunction<std::is_same<std::shared_ptr<NodeFunction>, std::decay_t<T>>..., std::is_same<FunctionItem, std::decay_t<T>>...>,
    std::disjunction<std::is_constructible<FunctionItem, T>, std::is_same<FunctionItem, std::decay_t<T>>>...>;

/**
 * @brief Operator which accepts parameters as long as they can be used to construct an FunctionItem.
 *        If both the LHS and RHS are std::shared_ptr<NodeFunction>s, this overload is not used.
 *
 * @dev   std::shared_ptr has got a non-explicit constructor for nullptr.
 *        The following construct is used to avoid implicit conversion of `0` to shared_ptr and to convert to an
 *        `FunctionItem` instead by providing a templated function which accepts all (single) arguments that can be
 *        used to construct an `FunctionItem`.
 *
 * @tparam LHS the type of the left-hand-side of the operator.
 * @tparam RHS the type of the right-hand-side of the operator.
 *
 * @param lhs  the value of the left-hand-side of the operator.
 * @param rhs  the value of the right-hand-side of the operator.
 *
 * @return std::shared_ptr<NodeFunction> which reflects the operator.
 */
template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator&&(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) && toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator||(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) || toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator==(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) == toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator!=(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) != toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator<=(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) <= toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator>=(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) >= toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator<(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) < toNodeFunctionPtr(std::forward<RHS>(rhs));
}

template <typename LHS, typename RHS, typename = std::enable_if_t<function_generator_v<LHS, RHS>>>
std::shared_ptr<NodeFunction> operator>(LHS&& lhs, RHS&& rhs)
{
    return toNodeFunctionPtr(std::forward<LHS>(lhs)) > toNodeFunctionPtr(std::forward<RHS>(rhs));
}

}
