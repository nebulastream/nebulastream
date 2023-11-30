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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_

#include <Optimizer/QueryMerger/BaseQueryMergerRule.hpp>
#include <Optimizer/QuerySignatures/SignatureContainmentCheck.hpp>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

namespace Optimizer {

class SignatureContainmentCheck;
using SignatureContainmentCheckPtr = std::shared_ptr<SignatureContainmentCheck>;

class Z3SignatureBasedBottomUpQueryContainmentRule;
using Z3SignatureBasedBottomUpQueryContainmentRulePtr = std::shared_ptr<Z3SignatureBasedBottomUpQueryContainmentRule>;

/**
 * @brief Z3SignatureBasedBottomUpQueryContainmentMergerRule utilizes containment relationships for the merging process.
 * It identifies equivalent subqueries and passes on the correct operators for merging.
 * Further, it identifies containment relationships among subqueries and passes on the correct operators for merging.
 * After running this rule only a single representative operator chain should exists in the Global Query Plan for the common
 * upstream operator chain.
 * Effectively this rule will prune the global query plan to remove duplicate and contained operators.
 *
 * Example:
 * Given a Global Query Plan with two Global Query Node chains as follow:
 *                                                         GQPRoot
 *                                                         /     \
 *                                                       /        \
 *                                                     /           \
 *                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
 *                                                |                 |
 *                                        GQN2({Map1},{Q1})    GQN7({Filter1},{Q2})
 *                                                |                 |
 *                                     GQN3({Filter2},{Q1})    GQN6({Map1},{Q2})
 *                                                |                 |
 *                                  GQN4({Source(Car)},{Q1})   GQN8({Source(Car)},{Q2})
 *
 *
 * where Filter1 contains Filter2.
 * After running the Z3SignatureBasedBottomUpQueryContainmentMergerRule, the resulting Global Query Plan will look as follow:
 *
*                                                         GQPRoot
*                                                         /     \
*                                                       /        \
*                                                     /           \
*                                         GQN1({Sink1},{Q1})  GQN5({Sink2},{Q2})
*                                                |                 |
*                                        GQN2({Map1},{Q1})    GQN7({Filter1},{Q2})
*                                                |                 |
*                                     GQN3({Filter2},{Q1})    GQN6({Map1},{Q2})
*                                                |                 |
 *                                            GQN4({Source(Car)},{Q1,Q2})
 *
 * Additionally information: As it traverses the query plan in a bottom up fashion an exits as soon as it identified a containment relationship
 * other than equivalent, the algorithm is not very good at identifying sharing abilities
 */
class Z3SignatureBasedBottomUpQueryContainmentRule final : public BaseQueryMergerRule {

  public:
    /**
     * @brief create an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     * @param context The Z3 context for the SMT solver
     * @return an instance of Z3SignatureBasedContainmentBasedCompleteQueryMergerRule
     */
    static Z3SignatureBasedBottomUpQueryContainmentRulePtr create(const z3::ContextPtr& context,
                                                                  bool allowExhaustiveContainmentCheck);

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
    explicit Z3SignatureBasedBottomUpQueryContainmentRule(const z3::ContextPtr& context, bool allowExhaustiveContainmentCheck);

    /**
     * @brief identify if the query plans are equal or not
     * @param targetQueryPlan : target query plan
     * @param hostQueryPlan : host query plan
     * @return Map containing matching pair of target and host operators
     */
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>>
    areQueryPlansContained(const QueryPlanPtr& targetQueryPlan, const QueryPlanPtr& hostQueryPlan);

    /**
     * @brief This method compares two operator signatures using Z3
     * @param targetOperator : the target operator to compare
     * @param hostOperator : the host operator to compare with
     * @return bool true if equal else false
     */
    std::map<LogicalOperatorNodePtr, std::tuple<LogicalOperatorNodePtr, ContainmentRelationship>>
    areOperatorsContained(const LogicalOperatorNodePtr& targetOperator, const LogicalOperatorNodePtr& hostOperator);

    /**
     * @brief This method makes sure that we can reset the time attribute for the contained window operation
     * @param container operation that contains the other
     * @param containee contained operation
     * @return true, if container and contanee are not a window operation or if window containment is possible, false otherwise
     */
    bool checkWindowContainmentPossible(const LogicalOperatorNodePtr& container, const LogicalOperatorNodePtr& containee) const;

    SignatureContainmentCheckPtr signatureContainmentUtil;
};
}// namespace Optimizer
}// namespace NES
#endif  // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYMERGER_Z3SIGNATUREBASEDBOTTOMUPQUERYCONTAINMENTRULE_HPP_
