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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include<Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical InferModel operator.
 */
class PhysicalInferModelOperator : public PhysicalUnaryOperator {
  public:
    PhysicalInferModelOperator(OperatorId id,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema,
                               std::string model,
                               std::vector<ExpressionItemPtr> inputFields,
                               std::vector<ExpressionItemPtr> outputFields,
                               InferModel::InferModelOperatorHandlerPtr operatorHandler);

    static PhysicalOperatorPtr create(OperatorId id,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      std::string model,
                                      std::vector<ExpressionItemPtr> inputFields,
                                      std::vector<ExpressionItemPtr> outputFields,
                                      InferModel::InferModelOperatorHandlerPtr operatorHandler);

    static PhysicalOperatorPtr create(SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      std::string model,
                                      std::vector<ExpressionItemPtr> inputFields,
                                      std::vector<ExpressionItemPtr> outputFields,
                                      InferModel::InferModelOperatorHandlerPtr operatorHandler);

    std::string toString() const override;
    OperatorNodePtr copy() override;
    const std::string& getModel() const;
    const std::vector<ExpressionItemPtr>& getInputFields() const;
    const std::vector<ExpressionItemPtr>& getOutputFields() const;
    InferModel::InferModelOperatorHandlerPtr getInferModelHandler();

  protected:
    const std::string model;
    const std::vector<ExpressionItemPtr> inputFields;
    const std::vector<ExpressionItemPtr> outputFields;
    InferModel::InferModelOperatorHandlerPtr operatorHandler;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_
