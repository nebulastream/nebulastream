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

class LogicalWindowDescriptor;
using LogicalWindowDefinitionPtr = std::shared_ptr<LogicalWindowDescriptor>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

}// namespace NES::Windowing

namespace NES::Join {
class JoinDescriptor;
using LogicalJoinDefinitionPtr = std::shared_ptr<JoinDescriptor>;

namespace Experimental {
class BatchJoinDescriptor;
using LogicalBatchJoinDefinitionPtr = std::shared_ptr<BatchJoinDescriptor>;

class BatchJoinOperatorHandler;
using BatchJoinOperatorHandlerPtr = std::shared_ptr<BatchJoinOperatorHandler>;
}// namespace Experimental
}// namespace NES::Join
namespace NES {

class LogicalOperator;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperator>;

class UnaryOperatorNode;
using UnaryOperatorNodePtr = std::shared_ptr<UnaryOperatorNode>;

class LogicalUnaryOperator;
using LogicalUnaryOperatorNodePtr = std::shared_ptr<LogicalUnaryOperator>;

class BinaryOperatorNode;
using BinaryOperatorNodePtr = std::shared_ptr<BinaryOperatorNode>;

class LogicalBinaryOperator;
using LogicalBinaryOperatorNodePtr = std::shared_ptr<LogicalBinaryOperator>;

class ExchangeOperatorNode;
using ExchangeOperatorNodePtr = std::shared_ptr<ExchangeOperatorNode>;

class LogicalSourceOperator;
using SourceLogicalOperatorNodePtr = std::shared_ptr<LogicalSourceOperator>;

class LogicalSinkOperator;
using SinkLogicalOperatorNodePtr = std::shared_ptr<LogicalSinkOperator>;

class LogicalFilterOperator;
using FilterLogicalOperatorNodePtr = std::shared_ptr<LogicalFilterOperator>;

class WindowOperator;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperator>;

class LogicalJoinOperator;
using JoinLogicalOperatorNodePtr = std::shared_ptr<LogicalJoinOperator>;

namespace Experimental {
class LogicalBatchJoinOperator;
using BatchJoinLogicalOperatorNodePtr = std::shared_ptr<LogicalBatchJoinOperator>;
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

class LogicalBroadcastOperator;
using BroadcastLogicalOperatorNodePtr = std::shared_ptr<LogicalBroadcastOperator>;

class LogicalSourceOperator;
using SourceLogicalOperatorNodePtr = std::shared_ptr<LogicalSourceOperator>;

class LogicalSinkOperator;
using SinkLogicalOperatorNodePtr = std::shared_ptr<LogicalSinkOperator>;

class LogicalWatermarkAssignerOperator;
using WatermarkAssignerLogicalOperatorNodePtr = std::shared_ptr<LogicalWatermarkAssignerOperator>;

class CentralWindowOperator;
using CentralWindowOperatorPtr = std::shared_ptr<CentralWindowOperator>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class LogicalBroadcastOperator;
using BroadcastLogicalOperatorNodePtr = std::shared_ptr<LogicalBroadcastOperator>;

}// namespace NES

#endif  // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
