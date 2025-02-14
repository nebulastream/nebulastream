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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationBuild.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorFusionPolicy.hpp>

namespace NES::QueryCompilation
{

std::shared_ptr<OperatorFusionPolicy> FuseNonPipelineBreakerPolicy::create()
{
    return std::make_shared<FuseNonPipelineBreakerPolicy>();
}

bool FuseNonPipelineBreakerPolicy::isFusible(std::shared_ptr<PhysicalOperators::PhysicalOperator> physicalOperator)
{
    return (
        NES::Util::instanceOf<PhysicalOperators::PhysicalMapOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalSelectionOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalProjectOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalStreamJoinBuildOperator>(physicalOperator)
        || NES::Util::instanceOf<PhysicalOperators::PhysicalAggregationBuild>(physicalOperator));
}
}
