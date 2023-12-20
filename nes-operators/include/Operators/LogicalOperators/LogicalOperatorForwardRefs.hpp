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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_

#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <memory>

namespace NES::Windowing {

class LogicalWindowDefinition;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDefinition>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

}// namespace NES::Windowing

namespace NES::Join {
class LogicalJoinDefinition;
using LogicalJoinDefinitionPtr = std::shared_ptr<LogicalJoinDefinition>;

namespace Experimental {
class LogicalBatchJoinDefinition;
using LogicalBatchJoinDefinitionPtr = std::shared_ptr<LogicalBatchJoinDefinition>;

class BatchJoinOperatorHandler;
using BatchJoinOperatorHandlerPtr = std::shared_ptr<BatchJoinOperatorHandler>;
}// namespace Experimental
}// namespace NES::Join
namespace NES {

class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class UnaryOperatorNode;
using UnaryOperatorNodePtr = std::shared_ptr<UnaryOperatorNode>;

class LogicalUnaryOperatorNode;
using LogicalUnaryOperatorNodePtr = std::shared_ptr<LogicalUnaryOperatorNode>;

class BinaryOperatorNode;
using BinaryOperatorNodePtr = std::shared_ptr<BinaryOperatorNode>;

class LogicalBinaryOperatorNode;
using LogicalBinaryOperatorNodePtr = std::shared_ptr<LogicalBinaryOperatorNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;

class WindowOperatorNode;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperatorNode>;

class JoinLogicalOperatorNode;
using JoinLogicalOperatorNodePtr = std::shared_ptr<JoinLogicalOperatorNode>;

namespace Experimental {
class BatchJoinLogicalOperatorNode;
using BatchJoinLogicalOperatorNodePtr = std::shared_ptr<BatchJoinLogicalOperatorNode>;
}// namespace Experimental

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

class ConstantValueExpressionNode;
using ConstantValueExpressionNodePtr = std::shared_ptr<ConstantValueExpressionNode>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

namespace Catalogs::UDF {
class JavaUDFDescriptor;
using JavaUDFDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;
}// namespace Catalogs::UDF

class BroadcastLogicalOperatorNode;
using BroadcastLogicalOperatorNodePtr = std::shared_ptr<BroadcastLogicalOperatorNode>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

class WatermarkAssignerLogicalOperatorNode;
using WatermarkAssignerLogicalOperatorNodePtr = std::shared_ptr<WatermarkAssignerLogicalOperatorNode>;

class CentralWindowOperator;
using CentralWindowOperatorPtr = std::shared_ptr<CentralWindowOperator>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class BroadcastLogicalOperatorNode;
using BroadcastLogicalOperatorNodePtr = std::shared_ptr<BroadcastLogicalOperatorNode>;

}// namespace NES

#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
