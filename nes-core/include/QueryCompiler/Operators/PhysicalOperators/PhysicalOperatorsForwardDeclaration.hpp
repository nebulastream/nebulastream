/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORSFORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORSFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

namespace Join {
class LogicalJoinDefinition;
using LogicalJoinDefinitionPtr = std::shared_ptr<LogicalJoinDefinition>;

class JoinOperatorHandler;
using JoinOperatorHandlerPtr = std::shared_ptr<JoinOperatorHandler>;
}// namespace Join

namespace Windowing {

class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;

}// namespace Windowing

namespace QueryCompilation {

namespace PhysicalOperators {

class PhysicalOperator;
using PhysicalOperatorPtr = std::shared_ptr<PhysicalOperator>;

class PhysicalFilterOperator;
using PhysicalFilterOperatorPtr = std::shared_ptr<PhysicalFilterOperator>;

class PhysicalMapOperator;
using PhysicalMapOperatorPtr = std::shared_ptr<PhysicalMapOperator>;

class PhysicalMultiplexOperator;
using PhysicalMultiplexOperatorPtr = std::shared_ptr<PhysicalMultiplexOperator>;

class PhysicalDemultiplexOperator;
using PhysicalDemultiplexOperatorPtr = std::shared_ptr<PhysicalDemultiplexOperator>;

class PhysicalProjectOperator;
using PhysicalProjectOperatorPtr = std::shared_ptr<PhysicalProjectOperator>;

class PhysicalSinkOperator;
using PhysicalSinkOperatorPtr = std::shared_ptr<PhysicalSinkOperator>;

class PhysicalSourceOperator;
using PhysicalSourceOperatorPtr = std::shared_ptr<PhysicalSourceOperator>;

class PhysicalUnaryOperator;
using PhysicalUnaryOperatorPtr = std::shared_ptr<PhysicalUnaryOperator>;

class PhysicalBinaryOperator;
using PhysicalBinaryOperatorPtr = std::shared_ptr<PhysicalBinaryOperator>;

class PhysicalWatermarkAssignmentOperator;
using PhysicalWatermarkAssignmentOperatorPtr = std::shared_ptr<PhysicalWatermarkAssignmentOperator>;

class PhysicalSliceMergingOperator;
using PhysicalSliceMergingOperatorPtr = std::shared_ptr<PhysicalSliceMergingOperator>;

class PhysicalSlicePreAggregationOperator;
using PhysicalSlicePreAggregationOperatorPtr = std::shared_ptr<PhysicalSlicePreAggregationOperator>;

class PhysicalSliceSinkOperator;
using PhysicalSliceSinkOperatorPtr = std::shared_ptr<PhysicalSliceSinkOperator>;

class PhysicalWindowSinkOperator;
using PhysicalWindowSinkOperatorPtr = std::shared_ptr<PhysicalWindowSinkOperator>;

class PhysicalJoinBuildOperator;
using PhysicalJoinBuildOperatorPtr = std::shared_ptr<PhysicalJoinBuildOperator>;

class PhysicalWatermarkAssignmentOperator;
using PhysicalWatermarkAssignmentOperatorPtr = std::shared_ptr<PhysicalWatermarkAssignmentOperator>;

class PhysicalJoinSinkOperator;
using PhysicalJoinSinkOperatorPtr = std::shared_ptr<PhysicalJoinSinkOperator>;

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATORSFORWARDDECLARATION_HPP_
