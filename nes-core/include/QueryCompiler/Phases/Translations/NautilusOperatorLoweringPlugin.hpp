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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/PluginRegistry.hpp>
#include <optional>

namespace NES::Runtime::Execution::Operators {
class ExecutableOperator;
using ExecutableOperatorPtr = std::shared_ptr<ExecutableOperator>;
}// namespace NES::Runtime::Execution::Operators

namespace NES::QueryCompilation {

/**
 * @brief Plugin interface to lower a physical operator to an nautilus implementation.
 */
class NautilusOperatorLoweringPlugin {
  public:
    NautilusOperatorLoweringPlugin() = default;

    /**
     * @brief Creates an executable nautilus operator for an specific physical operator and a operator handler.
     * @param physicalOperator physical operator
     * @param operatorHandlers operator handlers.
     * @return
     */
    virtual std::optional<Runtime::Execution::Operators::ExecutableOperatorPtr>
    lower(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) = 0;

    virtual ~NautilusOperatorLoweringPlugin() = default;
};

using NautilusOperatorLoweringPluginRegistry = Util::PluginRegistry<NautilusOperatorLoweringPlugin>;
}// namespace NES::QueryCompilation

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
