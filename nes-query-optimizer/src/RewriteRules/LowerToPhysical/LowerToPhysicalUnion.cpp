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

#include <memory>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <LogicalOperators/UnionOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalUnion.hpp>
#include <PhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <UnionPhysicalOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES::Optimizer
{

RewriteRuleResultSubgraph LowerToPhysicalUnion::apply(Logical::Operator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGet<Logical::UnionOperator>(), "Expected a UnionLogicalOperator");
    auto source = logicalOperator.get<Logical::UnionOperator>();

    auto inputSchemas = logicalOperator.getInputSchemas();
    PRECONDITION(inputSchemas.size() == 2, "UnionLogicalOperator should have exactly two schema, but has {}", inputSchemas.size());
    auto physicalOperator = UnionPhysicalOperator();
    /// TODO we take here just one schema as only one is supported by the physicaloperatorwrapper
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(physicalOperator, inputSchemas[0], logicalOperator.getOutputSchema());
    return {wrapper, {wrapper, wrapper}};
}

RewriteRuleRegistryReturnType RewriteRuleGeneratedRegistrar::RegisterUnionRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<LowerToPhysicalUnion>(argument.conf);
}
}
