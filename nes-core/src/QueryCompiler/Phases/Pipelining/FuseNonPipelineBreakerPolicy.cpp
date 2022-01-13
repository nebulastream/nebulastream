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
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperatorsForwardDeclaration.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/EventTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>

namespace NES::QueryCompilation {

OperatorFusionPolicyPtr FuseNonPipelineBreakerPolicy::create() { return std::make_shared<FuseNonPipelineBreakerPolicy>(); }

bool FuseNonPipelineBreakerPolicy::isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) {
    return (physicalOperator->instanceOf<PhysicalOperators::PhysicalMapOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalFilterOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalProjectOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalJoinBuildOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalSliceMergingOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalKeyedTumblingWindowSink>()
            || physicalOperator->instanceOf<PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator>()
                );
}
}// namespace NES::QueryCompilation