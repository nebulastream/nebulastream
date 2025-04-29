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
#include <Abstract/LogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>

namespace NES
{
namespace QueryCompilation
{
class FunctionProvider
{
public:
    /// Lowers a function node to a function by calling for each of its sub-functions recursively the lowerFunction until we reach
    < < < < < < < < HEAD : nes - execution / include / QueryCompiler / Phases / Translations / FunctionProvider.hpp
        /// NodeFunction a NodeFunctionConstantValue, FieldAccessLogicalFunction or FieldAssignment
        == == == ==
        /// NodeFunction a NodeFunctionConstantValue, NodeFunctionFieldAccess or FieldAssignment
        >>>>>>>> 90577b636a(refactor(PhysicalOperators)
                            : rename ExecutableFunction to PhysicalFunction)
        : nes - physical
        - operators / include / Functions
            / FunctionProvider.hpp
              static std::unique_ptr<Functions::PhysicalFunction> lowerFunction(const std::shared_ptr<LogicalFunction>& nodeFunction);

private:
    static std::unique_ptr<Functions::PhysicalFunction>
    lowerConstantFunction(const std::shared_ptr<ConstantValueLogicalFunction>& nodeFunction);
};

}
