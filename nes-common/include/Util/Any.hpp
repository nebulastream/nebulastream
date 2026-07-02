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
#include <boost/core/demangle.hpp>

namespace NES
{

/// Wraps std::any for use as the value type of std::expected. std::expected<std::any, E> itself is ill-formed with
/// clang + libstdc++: std::any is constructible from anything, including the expected itself, so the constraints on
/// expected's converting constructors (__cons_from_expected) recursively depend on themselves. Restricting the
/// constructor to exactly std::any makes this type not constructible from the surrounding expected, breaking the cycle.
/// Without the same_as constraint, any parameter would be accepted the expected via std::any's
/// implicit converting constructor, and checking that conversion re-enters the same constraint recursion.
class ExplicitAny
{
    std::any value;

public:
    template <std::same_as<std::any> A>
    explicit ExplicitAny(A value) : value(std::move(value))
    {
    }

    [[nodiscard]] const std::any& getValue() const { return value; }

    template <typename T>
    [[nodiscard]] T getAs() const {
            PRECONDITION(
                typeid(T) == value.type(),
                "Stored config type {} does not match requested type {}",
                boost::core::demangle(value.type().name()),
                boost::core::demangle(typeid(T).name()));
        return std::any_cast<T>(value);
    }
};
}