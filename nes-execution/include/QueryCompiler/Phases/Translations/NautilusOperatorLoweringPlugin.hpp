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
#include <optional>
#include <vector>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::QueryCompilation
{

/**
 * @brief Plugin interface to lower a physical operator to an nautilus implementation.
 */
class NautilusOperatorLoweringPlugin
{
public:
    NautilusOperatorLoweringPlugin() = default;

    /**
     * @brief Creates an executable nautilus operator for an specific physical operator and a operator handler.
     * @param physicalOperator physical operator
     * @param operatorHandlers operator handlers.
     * @return
     */
    virtual std::optional<std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>> lower(
        const std::shared_ptr<PhysicalOperators::PhysicalOperator>& physicalOperator,
        std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>& operatorHandlers)
        = 0;

    virtual ~NautilusOperatorLoweringPlugin() = default;
};
}
