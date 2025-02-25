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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>

namespace NES::QueryCompilation
{

OperatorFusionPolicyPtr FuseNonPipelineBreakerPolicy::create()
{
    return std::make_shared<FuseNonPipelineBreakerPolicy>();
}

bool FuseNonPipelineBreakerPolicy::isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator)
{
    return (
        NES::Util::instanceOf<PhysicalOperators::PhysicalMapOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalSelectionOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalProjectOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>(physicalOperator));
}
}
