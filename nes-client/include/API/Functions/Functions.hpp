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

#include <Functions/Expression.hpp>

namespace NES
{

/// This file contains the user facing api to create function nodes in a fluent and easy way.

/// A function item represents the leaf in an function tree.
/// It is converted to an constant value function or a field access function.
class FunctionItem
{
public:
    FunctionItem(int8_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(uint8_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(int16_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(uint16_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(int32_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(uint32_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(int64_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(uint64_t value); ///NOLINT(google-explicit-constructor)
    FunctionItem(float value); ///NOLINT(google-explicit-constructor)
    FunctionItem(double value); ///NOLINT(google-explicit-constructor)
    FunctionItem(bool value); ///NOLINT(google-explicit-constructor)
    /// ReSharper disable once CppNonExplicitConvertingConstructor
    FunctionItem(ExpressionValue exp); /// NOLINT(google-explicit-constructor)

    FunctionItem(const FunctionItem&) = default;
    FunctionItem(FunctionItem&&) = default;

    ExpressionValue /// NOLINT(misc-unconventional-assign-operator)
    operator=(const FunctionItem & assignItem) const;

    ExpressionValue /// NOLINT(misc-unconventional-assign-operator)
    operator=(const ExpressionValue assignFunction) const;

    /// Gets the function node of this function item.
    [[nodiscard]] ExpressionValue getNodeFunction() const;
    /// ReSharper disable once CppNonExplicitConversionOperator
    operator ExpressionValue(); /// NOLINT(google-explicit-constructor)

    /// Rename the function item
    /// @param name : the new name
    /// @return the updated function item
    FunctionItem as(std::string name);

private:
    ExpressionValue function;
};

/// Attribute(name) allows the user to reference a field in his function.
/// Attribute("f1") < 10
/// todo rename to field if conflict with legacy code is resolved.
/// @param fieldName
FunctionItem Attribute(std::string name);

}
