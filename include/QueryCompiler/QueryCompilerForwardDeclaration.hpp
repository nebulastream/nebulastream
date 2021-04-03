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
#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
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

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

namespace QueryCompilation {

class TranslateToPhysicalOperators;
typedef std::shared_ptr<TranslateToPhysicalOperators> TranslateToPhysicalOperatorsPtr;

class PhysicalOperatorProvider;
typedef std::shared_ptr<PhysicalOperatorProvider> PhysicalOperatorProviderPtr;

class PhysicalQueryPlan;
typedef std::shared_ptr<PhysicalQueryPlan> PhysicalQueryPlanPtr;

namespace PhysicalOperators{
class PhysicalOperator;
typedef std::shared_ptr<PhysicalOperator> PhysicalOperatorPtr;
}


}// namespace QueryCompilation

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
