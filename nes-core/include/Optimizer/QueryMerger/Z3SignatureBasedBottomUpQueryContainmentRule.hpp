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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {

class SignatureContainmentCheck;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentCheck>;

class Z3SignatureBasedBottomUpQueryContainmentRule;
using Z3SignatureBasedBottomUpQueryContainmentRulePtr = std::shared_ptr<Z3SignatureBasedBottomUpQueryContainmentRule>;

/**
 * @brief Z3SignatureBasedCompleteQueryMergerRule currently identifies containment relationships between the global query plan and newly registered queries
 */
class Z3SignatureBasedBottomUpQueryContainmentRule final : public BaseQueryMergerRule {

  public:
    /**
     * @brief create an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     * @param context The Z3 context for the SMT solver
     * @return an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     */
    static Z3SignatureBasedBottomUpQueryContainmentRulePtr create(const z3::ContextPtr& context);

    /**
     * @brief checks for containment between the globalQueryPlan and the currently newly added query
     * @param globalQueryPlan an instance of the global query plan
     * @return true if containment is present, false otherwise
     */
    bool apply(GlobalQueryPlanPtr globalQueryPlan) override;

    /**
     * @brief destructor
     */
    ~Z3SignatureBasedBottomUpQueryContainmentRule() noexcept final = default;

  private:
    /**
     * @brief explicit constructor
     * @param context The Z3 context for the SMT solver
     */
    explicit Z3SignatureBasedBottomUpQueryContainmentRule(const z3::ContextPtr& context);

    /**
     * @brief identify if the query plans are equal or not
     * @param targetQueryPlan : target query plan
     * @param hostQueryPlan : host query plan
     * @return Map containing matching pair of target and host operators
     */
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
    areQueryPlansContained(const QueryPlanPtr& targetQueryPlan, const QueryPlanPtr& hostQueryPlan);

    /**
     * @brief This method compares two operator signatures using Z3
     * @param targetOperator : the target operator to compare
     * @param hostOperator : the host operator to compare with
     * @return bool true if equal else false
     */
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentType>>
    areOperatorsContained(const LogicalOperatorNodePtr& targetOperator, const LogicalOperatorNodePtr& hostOperator);

    /**
     * @brief This method makes sure that we can reset the time attribute for the contained window operation
     * @param container operation that contains the other
     * @param containee contained operation
     * @return true, if container and contanee are not a window operation or if window containment is possible, false otherwise
     */
    bool checkWindowContainmentPossible(const LogicalOperatorNodePtr& container, const LogicalOperatorNodePtr& containee) const;

    SignatureContainmentUtilPtr signatureContainmentUtil;
};
}// namespace NES::Optimizer

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_
