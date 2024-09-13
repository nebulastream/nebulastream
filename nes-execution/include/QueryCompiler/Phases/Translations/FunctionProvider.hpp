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
#include <Functions/ConstantValueFunctionNode.hpp>

namespace NES
{
class FunctionNode;
using FunctionNodePtr = std::shared_ptr<FunctionNode>;
class FunctionFunction;

namespace Runtime::Execution::Functions
{
class Function;
using FunctionPtr = std::unique_ptr<Function>;
}

namespace QueryCompilation
{
class FunctionProvider
{
public:
    /// Lowers a function node to a function by calling for each of its sub-functions recursively the lowerFunction until we reach
    /// a ConstantValueFunctionNode, FieldAccessFunctionNode or FieldAssignmentFunctionNode
    static Runtime::Execution::Functions::FunctionPtr lowerFunction(const FunctionNodePtr& functionNode);


private:
    static Runtime::Execution::Functions::FunctionPtr lowerConstantFunction(const std::shared_ptr<ConstantValueFunctionNode>& functionNode);
};

}
}
