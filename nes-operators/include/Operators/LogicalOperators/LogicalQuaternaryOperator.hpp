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
#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/QuaternaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>

namespace NES {

/**
 * @brief Logical Quaternary operator, defines three output schemas
 */
class LogicalQuaternaryOperator : public QuaternaryOperator {
  public:
    LogicalQuaternaryOperator(const ExpressionNodePtr left,
                              const ExpressionNodePtr middle,
                              const ExpressionNodePtr right,
                              const std::string function);

    /**
    * @brief Infers the input and output schema of this operator depending on its children.
    * @throws Exception if the schema could not be inferred correctly or if the inferred types are not valid.
    * @return true if the schema was correctly inferred
    */
    virtual bool inferSchema();
    virtual void inferInputOrigins();
    virtual std::vector<OperatorPtr> getLeftOperators() const;
    virtual std::vector<OperatorPtr> getRightOperators() const;
    virtual OperatorPtr copy();

  private:
    std::vector<OperatorPtr> getOperatorsBySchema(const SchemaPtr& schema) const;

  protected:
    const ExpressionNodePtr left;
    const ExpressionNodePtr middle;
    const ExpressionNodePtr right;
    SchemaPtr leftInputSchema = Schema::create();
    SchemaPtr middleInputSchema = Schema::create();
    SchemaPtr rightInputSchema = Schema::create();
    SchemaPtr outputSchema = Schema::create();
    std::string function;
    std::vector<SchemaPtr> distinctSchemas;
    std::vector<OriginId> leftInputOriginIds;
    std::vector<OriginId> middleInputOriginIds;
    std::vector<OriginId> rightInputOriginIds;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_
