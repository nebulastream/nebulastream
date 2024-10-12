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
#include <QueryCompiler/QueryCompilerOptions.hpp>
namespace NES::QueryCompilation
{

/// This is a general interface, which provides the functionality to replace a logical operator with corresponding
/// physical operators.
class PhysicalOperatorProvider
{
public:
    explicit PhysicalOperatorProvider(std::shared_ptr<QueryCompilerOptions> options) : options(std::move(options)) {};
    virtual ~PhysicalOperatorProvider() = default;

    /// This method is called to replace the logical operator with corresponding physical operators.
    virtual void lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode) = 0;

protected:
    std::shared_ptr<QueryCompilerOptions> options;
};
using PhysicalOperatorProviderPtr = std::shared_ptr<PhysicalOperatorProvider>;
}
