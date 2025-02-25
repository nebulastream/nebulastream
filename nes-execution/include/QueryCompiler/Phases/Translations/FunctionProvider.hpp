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
#include <Functions/NodeFunctionConstantValue.hpp>

namespace NES
{
class NodeFunction;
using NodeFunctionPtr = std::shared_ptr<NodeFunction>;
class FunctionFunction;

namespace QueryCompilation
{
class FunctionProvider
{
public:
    /// Lowers a function node to a function by calling for each of its sub-functions recursively the lowerFunction until we reach
    ///NodeFunction a NodeFunctionConstantValue, NodeFunctionFieldAccess or FieldAssignment
    static std::unique_ptr<Runtime::Execution::Functions::Function> lowerFunction(const NodeFunctionPtr& nodeFunction);


private:
    static std::unique_ptr<Runtime::Execution::Functions::Function>
    lowerConstantFunction(const std::shared_ptr<NodeFunctionConstantValue>& nodeFunction);
};

}
}
