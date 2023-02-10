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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDCONTAINMENTBASEDCOMPLETEQUERYMERGERRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDCONTAINMENTBASEDCOMPLETEQUERYMERGERRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {

class SignatureContainmentUtil;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentUtil>;

class Z3SignatureBasedContainmentBasedCompleteQueryMergerRule;
using Z3SignatureBasedContainmentBasedCompleteQueryMergerRulePtr = std::shared_ptr<Z3SignatureBasedContainmentBasedCompleteQueryMergerRule>;

/**
 * @brief Z3SignatureBasedCompleteQueryMergerRule currently identifies containment relationships between the global query plan and newly registered queries
 */
class Z3SignatureBasedContainmentBasedCompleteQueryMergerRule final : public BaseQueryMergerRule {

  public:
    /**
     * @brief create an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     * @param context The Z3 context for the SMT solver
     * @return an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     */
    static Z3SignatureBasedContainmentBasedCompleteQueryMergerRulePtr create(const z3::ContextPtr& context);

    /**
     * @brief checks for containment between the globalQueryPlan and the currently newly added query
     * @param globalQueryPlan an instance of the global query plan
     * @return true if containment is present, false otherwise
     */
    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

    /**
     * @brief destructor
     */
    ~Z3SignatureBasedContainmentBasedCompleteQueryMergerRule() noexcept final = default;

    /**
     * @brief get an instance of the SignatureContainmentUtil
     * @return an instance of signatureContainmentUtil
     */
    const SignatureContainmentUtilPtr& getSignatureContainmentUtil() const;

  private:
    /**
     * @brief explicit constructor
     * @param context The Z3 context for the SMT solver
     */
    explicit Z3SignatureBasedContainmentBasedCompleteQueryMergerRule(const z3::ContextPtr& context);

    SignatureContainmentUtilPtr signatureContainmentUtil;

};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDCOMPLETEQUERYMERGERRULE_HPP_
