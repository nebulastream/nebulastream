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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_EXPERIMENTAL_VECTORIZATION_PHYSICALKERNELOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_EXPERIMENTAL_VECTORIZATION_PHYSICALKERNELOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Experimental/Vectorization/PhysicalVectorizedPipelineOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

/**
 * @brief Physical Kernel operator.
 */
class PhysicalKernelOperator : public PhysicalUnaryOperator {
public:
    PhysicalKernelOperator(OperatorId id, const std::shared_ptr<PhysicalVectorizedPipelineOperator>&);

    static PhysicalOperatorPtr create(const std::shared_ptr<PhysicalVectorizedPipelineOperator>& vectorizedPipeline);

    std::string toString() const override;

    OperatorNodePtr copy() override;

    const std::shared_ptr<PhysicalVectorizedPipelineOperator>& getVectorizedPipeline();

private:
    std::shared_ptr<PhysicalVectorizedPipelineOperator> vectorizedPipeline;
};

}// namespace NES::QueryCompilation::PhysicalOperators::Experimental

#endif // NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_EXPERIMENTAL_VECTORIZATION_PHYSICALKERNELOPERATOR_HPP_
