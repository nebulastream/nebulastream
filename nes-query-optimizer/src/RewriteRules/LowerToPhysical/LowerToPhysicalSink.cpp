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

#include <vector>
#include <memory>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Functions/FunctionProvider.hpp>
#include <SinkPhysicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/LowerToPhysical/LowerToPhysicalSink.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>

namespace NES::Optimizer
{

std::vector<PhysicalOperatorWrapper> LowerToPhysicalSink::apply(LogicalOperator logicalOperator)
{
    auto sink = *logicalOperator.get<SinkLogicalOperator>();
    auto physicalOperator = SinkPhysicalOperator(sink.sinkDescriptor);
    auto wrapper = PhysicalOperatorWrapper(physicalOperator, sink.getInputSchemas()[0], sink.getOutputSchema());
    return {wrapper};
}

std::unique_ptr<Optimizer::AbstractRewriteRule> RewriteRuleGeneratedRegistrar::RegisterSinkRewriteRule(RewriteRuleRegistryArguments argument)
{
    return std::make_unique<NES::Optimizer::LowerToPhysicalSink>(argument.conf);
}
}
