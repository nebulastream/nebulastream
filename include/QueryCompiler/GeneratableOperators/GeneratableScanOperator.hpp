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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
namespace NES {

class GeneratableScanOperator : public UnaryOperatorNode, public GeneratableOperator {
  public:
    /**
     * @brief Create sharable instance of GeneratableScanOperator
     * @param schema: the schema of the input records
     * @param schema: the schema of the output records
     * @param id: the operator id if not provided then next available operator id is used.
     * @return instance of GeneratableScanOperator
     */
    static GeneratableScanOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                             OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
    * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief Consume function, which generates code for the processing and calls the parent consume function.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
     * @brief To string method for the operator.
     * @return string
     */
    const std::string toString() const override;

    /**
    * Create a shallow copy of the operator by copying its operator properties but not its children or parent operator tree.
    * @return shallow copy of the operator
    */
    OperatorNodePtr copy() override;

  private:
    explicit GeneratableScanOperator(SchemaPtr inputSchema, SchemaPtr outputSchema, OperatorId id);
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
