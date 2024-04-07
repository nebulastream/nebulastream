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

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class FieldAssignmentExpressionNode;
using FieldAssignmentExpressionNodePtr = std::shared_ptr<FieldAssignmentExpressionNode>;

class LogicalFilterOperator;
using LogicalFilterOperatorPtr = std::shared_ptr<LogicalFilterOperator>;

class LogicalJoinOperator;
using LogicalJoinOperatorPtr = std::shared_ptr<LogicalJoinOperator>;

namespace Experimental {
class LogicalBatchJoinOperator;
using LogicalBatchJoinOperatorPtr = std::shared_ptr<LogicalBatchJoinOperator>;
}// namespace Experimental

class LogicalUnionOperator;
using LogicalUnionOperatorPtr = std::shared_ptr<LogicalUnionOperator>;

class LogicalProjectionOperator;
using LogicalProjectionOperatorPtr = std::shared_ptr<LogicalProjectionOperator>;

class LogicalMapOperator;
using LogicalMapOperatorPtr = std::shared_ptr<LogicalMapOperator>;

class LogicalWindowOperator;
using LogicalWindowOperatorPtr = std::shared_ptr<LogicalWindowOperator>;

class LogicalLimitOperator;
using LogicalLimitOperatorPtr = std::shared_ptr<LogicalLimitOperator>;

class WatermarkAssignerLogicalOperator;
using WatermarkAssignerLogicalOperatorPtr = std::shared_ptr<WatermarkAssignerLogicalOperator>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

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
using LogicalInferModelOperatorPtr = std::shared_ptr<LogicalInferModelOperator>;

class InferModelOperatorHandler;
using InferModelOperatorHandlerPtr = std::shared_ptr<InferModelOperatorHandler>;
}// namespace InferModel

namespace Statistic {
class LogicalStatisticWindowOperator;
using LogicalStatisticWindowOperatorPtr = std::shared_ptr<LogicalStatisticWindowOperator>;
} // namespace Statistic

}// namespace NES
#endif // NES_OPERATORS_INCLUDE_OPERATORS_OPERATORFORWARDDECLARATION_HPP_
