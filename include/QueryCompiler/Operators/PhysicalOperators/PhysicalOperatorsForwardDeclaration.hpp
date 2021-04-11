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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHYSICALOPERATORS_QUERYCOMPILERFORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHYSICALOPERATORS_QUERYCOMPILERFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

namespace Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;

class JoinOperatorHandler;
typedef std::shared_ptr<JoinOperatorHandler> JoinOperatorHandlerPtr;
}// namespace NES::Join

namespace Windowing {

class LogicalWindowDefinition;
typedef std::shared_ptr<LogicalWindowDefinition> LogicalWindowDefinitionPtr;

class WindowOperatorHandler;
typedef std::shared_ptr<WindowOperatorHandler> WindowOperatorHandlerPtr;

class WatermarkStrategyDescriptor;
typedef std::shared_ptr<WatermarkStrategyDescriptor> WatermarkStrategyDescriptorPtr;

}// namespace NES::Windowing


namespace QueryCompilation {

namespace PhysicalOperators{

class PhysicalOperator;
typedef std::shared_ptr<PhysicalOperator> PhysicalOperatorPtr;

class PhysicalFilterOperator;
typedef std::shared_ptr<PhysicalFilterOperator> PhysicalFilterOperatorPtr;

class PhysicalMapOperator;
typedef std::shared_ptr<PhysicalMapOperator> PhysicalMapOperatorPtr;

class PhysicalMultiplexOperator;
typedef std::shared_ptr<PhysicalMultiplexOperator> PhysicalMultiplexOperatorPtr;

class PhysicalDemultiplexOperator;
typedef std::shared_ptr<PhysicalDemultiplexOperator> PhysicalDemultiplexOperatorPtr;

class PhysicalProjectOperator;
typedef std::shared_ptr<PhysicalProjectOperator> PhysicalProjectOperatorPtr;

class PhysicalSinkOperator;
typedef std::shared_ptr<PhysicalSinkOperator> PhysicalSinkOperatorPtr;

class PhysicalSourceOperator;
typedef std::shared_ptr<PhysicalSourceOperator> PhysicalSourceOperatorPtr;

class PhysicalUnaryOperator;
typedef std::shared_ptr<PhysicalUnaryOperator> PhysicalUnaryOperatorPtr;

class PhysicalBinaryOperator;
typedef std::shared_ptr<PhysicalBinaryOperator> PhysicalBinaryOperatorPtr;

class PhysicalWatermarkAssignmentOperator;
typedef std::shared_ptr<PhysicalWatermarkAssignmentOperator> PhysicalWatermarkAssignmentOperatorPtr;

class PhysicalSliceMergingOperator;
typedef std::shared_ptr<PhysicalSliceMergingOperator> PhysicalSliceMergingOperatorPtr;

class PhysicalSlicePreAggregationOperator;
typedef std::shared_ptr<PhysicalSlicePreAggregationOperator> PhysicalSlicePreAggregationOperatorPtr;

class PhysicalSliceSinkOperator;
typedef std::shared_ptr<PhysicalSliceSinkOperator> PhysicalSliceSinkOperatorPtr;

class PhysicalWindowSinkOperator;
typedef std::shared_ptr<PhysicalWindowSinkOperator> PhysicalWindowSinkOperatorPtr;

class PhysicalJoinBuildOperator;
typedef std::shared_ptr<PhysicalJoinBuildOperator> PhysicalJoinBuildOperatorPtr;

class PhysicalWatermarkAssignmentOperator;
typedef std::shared_ptr<PhysicalWatermarkAssignmentOperator> PhysicalWatermarkAssignmentOperatorPtr;

class PhysicalJoinSinkOperator;
typedef std::shared_ptr<PhysicalJoinSinkOperator> PhysicalJoinSinkOperatorPtr;


}


}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_PHYSICALOPERATORS_QUERYCOMPILERFORWARDDECLARATION_HPP_
