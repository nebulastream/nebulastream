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
using LogicalWindowDescriptorPtr = std::shared_ptr<LogicalWindowDescriptor>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

}// namespace NES::Windowing

namespace NES::Join {
class LogicalJoinDescriptor;
using LogicalJoinDescriptorPtr = std::shared_ptr<LogicalJoinDescriptor>;

namespace Experimental {
class LogicalBatchJoinDescriptor;
using LogicalBatchJoinDescriptorPtr = std::shared_ptr<LogicalBatchJoinDescriptor>;

class BatchJoinOperatorHandler;
using BatchJoinOperatorHandlerPtr = std::shared_ptr<BatchJoinOperatorHandler>;
}// namespace Experimental
}// namespace NES::Join
namespace NES {

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

class UnaryOperator;
using UnaryOperatorPtr = std::shared_ptr<UnaryOperator>;

class LogicalUnaryOperator;
using LogicalUnaryOperatorPtr = std::shared_ptr<LogicalUnaryOperator>;

class BinaryOperator;
using BinaryOperatorPtr = std::shared_ptr<BinaryOperator>;

class LogicalBinaryOperator;
using LogicalBinaryOperatorPtr = std::shared_ptr<LogicalBinaryOperator>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class LogicalFilterOperator;
using LogicalFilterOperatorPtr = std::shared_ptr<LogicalFilterOperator>;

class WindowOperator;
using WindowOperatorPtr = std::shared_ptr<WindowOperator>;

class LogicalJoinOperator;
using LogicalJoinOperatorPtr = std::shared_ptr<LogicalJoinOperator>;

namespace Experimental {
class LogicalBatchJoinOperator;
using LogicalBatchJoinOperatorPtr = std::shared_ptr<LogicalBatchJoinOperator>;
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

class BroadcastLogicalOperator;
using BroadcastLogicalOperatorPtr = std::shared_ptr<BroadcastLogicalOperator>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class WatermarkAssignerLogicalOperator;
using WatermarkAssignerLogicalOperatorPtr = std::shared_ptr<WatermarkAssignerLogicalOperator>;

class CentralWindowOperator;
using CentralWindowOperatorPtr = std::shared_ptr<CentralWindowOperator>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class BroadcastLogicalOperator;
using BroadcastLogicalOperatorPtr = std::shared_ptr<BroadcastLogicalOperator>;

namespace Statistic {
class LogicalStatisticWindowOperator;
using LogicalStatisticWindowOperatorPtr = std::shared_ptr<LogicalStatisticWindowOperator>;
} // namespace Statistic

}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORFORWARDREFS_HPP_
