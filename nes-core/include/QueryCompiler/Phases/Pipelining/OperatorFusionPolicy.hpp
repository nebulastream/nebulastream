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
#ifndef NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORFUSIONPOLICY_HPP_
#define NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORFUSIONPOLICY_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Policy which determines if an particular operator can be fused with other operators in a pipeline.
 * A particular policy can make this secession based on rules or costs.
 */
class OperatorFusionPolicy {
  public:
    virtual ~OperatorFusionPolicy() = default;
    /**
     * @brief Determines if an particular operator can be fused with other operators.
     * @param physicalOperator a particular physical operator.
     * @return true if the operator is fusible
     */
    virtual bool isFusible(PhysicalOperators::PhysicalOperatorPtr physicalOperator) = 0;
};
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERYCOMPILER_PHASES_PIPELINING_OPERATORFUSIONPOLICY_HPP_
