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

#ifndef NES_INCLUDE_OPTIMIZER_PHASES_SIGNATUREINFERENCEPHASE_HPP_
#define NES_INCLUDE_OPTIMIZER_PHASES_SIGNATUREINFERENCEPHASE_HPP_

#include <Optimizer/Phases/QueryMergerPhase.hpp>
#include <memory>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;
}// namespace NES

namespace NES::Optimizer {

class SignatureInferencePhase;
using SignatureInferencePhasePtr = std::shared_ptr<SignatureInferencePhase>;

/**
 * @brief This class is responsible for computing the Z3 expression for all operators within a query
 */
class SignatureInferencePhase {

  public:
    /**
     * @brief Create instance of SignatureInferencePhase class
     * @param context : the z3 context
     * @param queryMergerRule: query merger rule type
     * @return pointer to the signature inference phase
     */
    static SignatureInferencePhasePtr create(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRule);

    /**
     * @brief this method will compute the Z3 expression for all operators of the input query plan
     * @param queryPlan: the input query plan
     */
    void execute(const QueryPlanPtr& queryPlan);

    /**
     * @brief Get shared instance of z3 context
     * @return context
     */
    [[nodiscard]] z3::ContextPtr getContext() const;

  private:
    /**
     * @brief Create instance of SignatureInferencePhase class
     * @param context : the z3 context
     * @param queryMergerRule : query merger rule type
     */
    explicit SignatureInferencePhase(z3::ContextPtr context, Optimizer::QueryMergerRule queryMergerRule);
    z3::ContextPtr context;
    Optimizer::QueryMergerRule queryMergerRule;
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_PHASES_SIGNATUREINFERENCEPHASE_HPP_
