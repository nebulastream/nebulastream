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

#ifndef NES_PHYSICALSTREAMJOINSINKOPERATOR_HPP
#define NES_PHYSICALSTREAMJOINSINKOPERATOR_HPP

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

class PhysicalStreamJoinSinkOperator : public PhysicalStreamJoinOperator, public PhysicalBinaryOperator, public AbstractScanOperator {

  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& leftSchema,
                                      const SchemaPtr& rightSchema,
                                      const SchemaPtr& outputSchema,
                                      const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler);

    static PhysicalOperatorPtr create(const SchemaPtr& leftSchema,
                                     const SchemaPtr& rightSchema,
                                     const SchemaPtr& outputSchema,
                                     const Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr& operatorHandler);


    PhysicalStreamJoinSinkOperator(OperatorId id,
                                   SchemaPtr leftSchema,
                                   SchemaPtr rightSchema,
                                   SchemaPtr outputSchema,
                                   Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr operatorHandler);

    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif//NES_PHYSICALSTREAMJOINSINKOPERATOR_HPP
