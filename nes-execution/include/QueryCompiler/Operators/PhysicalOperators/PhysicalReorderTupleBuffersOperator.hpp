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

#ifndef NES_PHYSICALREORDERTUPLEBUFFERSOPERATOR_HPP
#define NES_PHYSICALREORDERTUPLEBUFFERSOPERATOR_HPP

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief
 */
class PhysicalReorderTupleBuffersOperator : public PhysicalUnaryOperator, public AbstractScanOperator, public AbstractEmitOperator {
  public:
    PhysicalReorderTupleBuffersOperator(OperatorId id,
                                        StatisticId statisticId,
                                        SchemaPtr inputSchema,
                                        SchemaPtr outputSchema);
    static PhysicalOperatorPtr create(OperatorId id,
                                      StatisticId statisticId,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema);
    static PhysicalOperatorPtr create(StatisticId statisticId,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema);
    std::string toString() const override;
    OperatorPtr copy() override;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif//NES_PHYSICALREORDERTUPLEBUFFERSOPERATOR_HPP
