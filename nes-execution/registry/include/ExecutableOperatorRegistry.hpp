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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Util/Registry.hpp>

namespace NES::Runtime::Execution::Operators
{

using ExecutableOperatorRegistryReturnType = std::shared_ptr<ExecutableOperator>;
struct ExecutableOperatorRegistryArguments
{
    std::shared_ptr<NES::QueryCompilation::PhysicalOperators::PhysicalOperator> physical;
    std::vector<std::shared_ptr<OperatorHandler>>& operatorHandlers;
};

class ExecutableOperatorRegistry : public BaseRegistry<
                                       ExecutableOperatorRegistry,
                                       std::string,
                                       ExecutableOperatorRegistryReturnType,
                                       ExecutableOperatorRegistryArguments>
{
};
}


#define INCLUDED_FROM_REGISTRY_EXECUTABLE_OPERATOR
#include <ExecutableOperatorGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_EXECUTABLE_OPERATOR
