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
#include <utility>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>

namespace NES::QueryCompilation
{

/// This is a general interface, which provides the functionality to replace a logical operator with corresponding
/// physical operators.
class PhysicalOperatorProvider
{
public:
    explicit PhysicalOperatorProvider(Configurations::QueryCompilerConfiguration queryCompilerConfig)
        : queryCompilerConfig(std::move(queryCompilerConfig)) { };
    virtual ~PhysicalOperatorProvider() = default;

    /// This method is called to replace the logical operator with corresponding physical operators.
    virtual void lower(const DecomposedQueryPlan& decomposedQueryPlan, std::shared_ptr<LogicalOperator> operatorNode) = 0;

protected:
    Configurations::QueryCompilerConfiguration queryCompilerConfig;
};
}
