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
#include <string>
#include <vector>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

/// This file contains the user facing api to create function nodes in a fluent and easy way.

class NodeFunction;
using NodeFunctionPtr = std::shared_ptr<NodeFunction>;

class NodeFunctionFieldAssignment;
using NodeFunctionFieldAssignmentPtr = std::shared_ptr<NodeFunctionFieldAssignment>;

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
    FunctionItem(NodeFunctionPtr exp);

    FunctionItem(FunctionItem const&) = default;
    FunctionItem(FunctionItem&&) = default;

    NodeFunctionFieldAssignmentPtr operator=(FunctionItem assignItem);
    NodeFunctionFieldAssignmentPtr operator=(NodeFunctionPtr assignFunction);

    /// Gets the function node of this function item.
    [[nodiscard]] NodeFunctionPtr getNodeFunction() const;
    operator NodeFunctionPtr();

    /// Rename the function item
    /// @param name : the new name
    /// @return the updated function item
    FunctionItem as(std::string name);

private:
    NodeFunctionPtr function;
};

/// Attribute(name) allows the user to reference a field in his function.
/// Attribute("f1") < 10
/// todo rename to field if conflict with legacy code is resolved.
/// @param fieldName
FunctionItem Attribute(std::string name);

/// Attribute(name, type) allows the user to reference a field, with a specific type in his function.
/// Field("f1", Int) < 10.
/// todo remove this case if we added type inference at Runtime from the operator tree.
/// todo rename to field if conflict with legacy code is resolved.
/// @param fieldName, type
FunctionItem Attribute(std::string name, BasicType type);

/// WHEN(condition,value) can only be used as part of the left vector in a CASE() function.
/// Allows to only return the value function if condition is met.
/// @param conditionExp : a logical condition which will be evaluated.
/// @param valueExp : the value to return if the condition is the first true one.
NodeFunctionPtr WHEN(const NodeFunctionPtr& conditionExp, const NodeFunctionPtr& valueExp);
NodeFunctionPtr WHEN(FunctionItem conditionExp, NodeFunctionPtr valueExp);
NodeFunctionPtr WHEN(NodeFunctionPtr conditionExp, FunctionItem valueExp);
NodeFunctionPtr WHEN(FunctionItem conditionExp, FunctionItem valueExp);

/// CASE({WHEN(condition,value),WHEN(condition, value)} , value) allows to evaluate all
/// WHEN functions from the vector list and only return the first one where the condition evaluated to true, or the value of the default function.
/// The CASE({WHEN()},default) is evaluated as a concatenated ternary operator in C++.
/// @param whenFunctions : a vector of at least one WHEN function to evaluate.
/// @param defaultValueExp : an function which will be returned if no WHEN condition evaluated to true.
NodeFunctionPtr CASE(const std::vector<NodeFunctionPtr>& whenFunctions, NodeFunctionPtr defaultValueExp);
NodeFunctionPtr CASE(std::vector<NodeFunctionPtr> whenFunctions, FunctionItem defaultValueExp);

}
