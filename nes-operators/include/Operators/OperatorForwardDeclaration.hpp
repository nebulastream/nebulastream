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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_OPERATORFORWARDDECLARATION_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_OPERATORFORWARDDECLARATION_HPP_

#include <memory>
namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

class LogicalFilterOperator;
using FilterLogicalOperatorNodePtr = std::shared_ptr<LogicalFilterOperator>;

class LogicalJoinOperator;
using JoinLogicalOperatorNodePtr = std::shared_ptr<LogicalJoinOperator>;

namespace Experimental {
class LogicalBatchJoinOperator;
using BatchJoinLogicalOperatorNodePtr = std::shared_ptr<LogicalBatchJoinOperator>;
}// namespace Experimental

class LogicalUnionOperator;
using UnionLogicalOperatorNodePtr = std::shared_ptr<LogicalUnionOperator>;

class LogicalProjectionOperator;
using ProjectionLogicalOperatorNodePtr = std::shared_ptr<LogicalProjectionOperator>;

class LogicalMapOperator;
using MapLogicalOperatorNodePtr = std::shared_ptr<LogicalMapOperator>;

class LogicalWindowOperator;
using WindowLogicalOperatorNodePtr = std::shared_ptr<LogicalWindowOperator>;

class LogicalLimitOperator;
using LimitLogicalOperatorNodePtr = std::shared_ptr<LogicalLimitOperator>;

class LogicalWatermarkAssignerOperator;
using WatermarkAssignerLogicalOperatorNodePtr = std::shared_ptr<LogicalWatermarkAssignerOperator>;

class LogicalSourceOperator;
using SourceLogicalOperatorNodePtr = std::shared_ptr<LogicalSourceOperator>;

namespace Catalogs::UDF {
class JavaUDFDescriptor;
using JavaUdfDescriptorPtr = std::shared_ptr<JavaUDFDescriptor>;

class PythonUDFDescriptor;
using PythonUDFDescriptorPtr = std::shared_ptr<PythonUDFDescriptor>;

class UDFDescriptor;
using UDFDescriptorPtr = std::shared_ptr<UDFDescriptor>;
}// namespace Catalogs::UDF

namespace InferModel {
class LogicalInferModelOperator;
using InferModelLogicalOperatorNodePtr = std::shared_ptr<LogicalInferModelOperator>;

class InferModelOperatorHandler;
using InferModelOperatorHandlerPtr = std::shared_ptr<InferModelOperatorHandler>;
}// namespace InferModel

}// namespace NES
#endif  // NES_OPERATORS_INCLUDE_OPERATORS_OPERATORFORWARDDECLARATION_HPP_
