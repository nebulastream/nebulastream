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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATURECONTAINMENTUTIL_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATURECONTAINMENTUTIL_HPP_

#include "ExtractContainedOperatorsUtil.hpp"
#include <memory>
#include <z3++.h>

namespace z3 {
class solver;
using SolverPtr = std::shared_ptr<solver>;

class context;
using ContextPtr = std::shared_ptr<context>;

class expr;
using ExprPtr = std::shared_ptr<expr>;
}// namespace z3

namespace NES {
class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;

class FilterLogicalOperatorNode;
using FilterLogicalOperatorNodePtr = std::shared_ptr<FilterLogicalOperatorNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;
}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

class SignatureContainmentCheck;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentCheck>;
/**
 * @brief enum describing the given containment relationship
 */
enum class ContainmentType : uint8_t { NO_CONTAINMENT, LEFT_SIG_CONTAINED, RIGHT_SIG_CONTAINED, EQUALITY };

/**
 * @brief This is a utility to compare two signatures
 */
class SignatureContainmentCheck : public ExtractContainedOperatorsUtil {

  public:
    /**
     * @brief creates an instance of the SignatureContainmentUtil
     * @param context The Z3 context for the SMT solver
     * @return instance of SignatureContainmentUtil
     */
    static SignatureContainmentUtilPtr create(const z3::ContextPtr& context);

    /**
     * @brief constructor for signatureContainmentUtil
     * @param context The Z3 context for the SMT solver
     */
    explicit SignatureContainmentCheck(const z3::ContextPtr& context);

    /**
     * @brief Check containment relationships for the given signatures as follows
     *      First check for WindowContainment
     *      In case of window equality, we continue to check for projection containment
     *      In case of projection equality, we finally check for filter containment
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkContainmentForBottomUpMerging(const QuerySignaturePtr& leftSignature,
                                                       const QuerySignaturePtr& rightSignature);

    /**
     * @brief Check containment relationships for the given signatures as follows
     *      First check for WindowContainment
     *      In case of window equality, we continue to check for projection containment
     *      In case of projection equality, we finally check for filter containment
     *      We assume that a merging algorithm checks the containment relationships in a top-down manner and merges the
     *      upstream operator chains in case of an equivalence. In case of a containment relationship, we extract the contained operators
     *      and add them to the container's upstream operator chain
     * @param leftOperator the current operator of the left query
     * @param rightOperator the current operator of the right query
     * @return tuple with the containment relationship and a vector of the contained upstream operators. The vector is empty if
     * equivalence or no containment was detected.
     */
    std::tuple<ContainmentType, std::vector<LogicalOperatorNodePtr>>
    checkContainmentRelationshipForTopDownMerging(const LogicalOperatorNodePtr& leftOperator, const LogicalOperatorNodePtr& rightOperator);

  private:
    /**
     * @brief check for projection containment as follows:
     * if (!rightFOL && leftFOL == unsat, aka leftFOL ⊆ rightFOL
     *      && filters are equal)
     *        if (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
     *            true: return Equality
     *      true: return Right sig contained
     * else if (rightFOL && !leftFOL == unsat, aka rightFOL ⊆ leftFOL)
     *      && filters are equal
     *      true: return Left sig contained
     * else: No_Containment
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkProjectionContainment(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

    /**
     * @brief check for filter containment as follows:
     * check if right sig ⊆ left sig for filters
     *      true: check if left sig ⊆ right sig
     *          true: return EQUALITY
     *          false: return RIGHT_SIG_CONTAINED
     *      false: check if left sig ⊆ right sig
     *          true: return LEFT_SIG_CONTAINED
     *      false: return NO_CONTAINMENT
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkFilterContainment(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

    /**
     * @brief check for window containment as follows:
     * check if window conditions are present, if not, return EQUALITY
     * for the size of the smaller window:
     *          true: check if # of aggregates equal
     *              true: check if right sig ⊆ left sig
     *                  true: check if left sig ⊆ right sig
     *                      true: return EQUALITY
     *                      false: check if window containment possible
     *                      && equal projections
     *                         true: RIGHT_SIG_CONTAINED
     *                         false: NO_CONTAINMENT
     *              false: check if left sig ⊆ right sig
     *                  && check if window containment possible
*                       && equal projections
     *                      true: LEFT_SIG_CONTAINED
     *                      false: NO_CONTAINMENT
     *          false: check if leftWindow has more aggregates
     *              true: check if left sig ⊆ right sig
     *                  true: RIGHT_SIG_CONTAINED
     *                  false: NO_CONTAINMENT
     *              false: check if right sig ⊆ left sig
     *                  true: LEFT_SIG_CONTAINED
     *                  false: NO_CONTAINMENT
     *       if containmentRelationship != EQUALITY: break loop
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkWindowContainment(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

    /**
     * @brief Get the name of the field manipulated by the Map operator
     * @param Operator pointer
     * @return name of the field
     */
    std::string getFieldNameUsedByMapOperator(const NodePtr& node);

    /**
     * @brief creates conditions for checking projection containment:
     * if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
     * else we indicate that the attribute is involved in the projection as attributeStingName == true
     * all FOL are added to the projectionCondition vector
     * @param signature Query signature to extract conditions from
     * @param projectionFOL z3 expression vector to add conditions to
     */
    void createProjectionFOL(const QuerySignaturePtr& signature, z3::expr_vector& projectionFOL);

    /**
     * @brief checks if the combination (combined via &&) of negated conditions and non negated conditions is unsatisfiable
     * it also pops the given number of conditions and calls resetSolver() if the counter hits the threshold for resetting
     * @param negatedCondition condition that will be negated
     * @param condition condition that will just be added to the solver as it is
     * @return true if the combination of the given conditions is unsatisfiable, false otherwise
     */
    bool checkContainmentConditionsUnsatisfied(z3::expr_vector& negatedCondition, z3::expr_vector& condition);

    /**
     * @brief checks if window containment is possible, i.e. no join window, no avg or median aggregation, and filter conditions are equal
     * further, window-time-size and window-time-slide must be equal
     * @param leftWindow current window to be checked for join or aggregation
     * @param leftSignature left query signature for filter check
     * @param rightSignature right query signature for filter check
     * @return true if all conditions for window containment are met, false otherwise
     */
    bool checkWindowContainmentPossible(const std::map<std::string, z3::ExprPtr>& containerWindow,
                                        const std::map<std::string, z3::ExprPtr>& containedWindow,
                                        const QuerySignaturePtr& leftSignature,
                                        const QuerySignaturePtr& rightSignature);

    /**
     * @brief checks whether for the remaining attributes all map and aggregation functions are equal
     * @param leftSignature left query signature for transformation check
     * @param rightSignature right query signature for transformation check
     * @return true if same transformations, false otherwise
     */
    bool checkForEqualTransformations(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

    /**
     * @brief combines window and projection FOLs
     * @param leftSignature left query signature
     * @param rightSignature right query signature
     * @param leftQueryWindowFOL expression vector for left query
     * @param rightQueryWindowFOL expression vector for right query
     */
    void combineWindowAndProjectionFOL(const QuerySignaturePtr& leftSignature,
                                       const QuerySignaturePtr& rightSignature,
                                       z3::expr_vector& leftQueryWindowFOL,
                                       z3::expr_vector& rightQueryWindowFOL);

    /**
     * @brief checks if a smaller attribute list is contained in a larger attribute
     * @param leftSignature signature of the container candidate
     * @param rightSignature signature of the containee candidate
     * @return true if order is retained, false otherwise
     */
    bool checkAttributeOrder(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature) const;

    /**
     * @brief prevents false positives in case unions are present
     * @param container container signature
     * @param containee contained signature
     * @return true, if no union is present or all unions have an equal amount of filters per attribute, false otherwise
     */
    bool checkFilterContainmentPossible(const QuerySignaturePtr& container, const QuerySignaturePtr& containee);

    /**
     * @brief Reset z3 solver
     */
    void resetSolver();

    z3::ContextPtr context;
    z3::SolverPtr solver;
    uint64_t counter;
    const uint16_t RESET_SOLVER_THRESHOLD = 20050;
    const uint8_t NUMBER_OF_CONDITIONS_TO_POP_FROM_SOLVER = 2;
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATURECONTAINMENTUTIL_HPP_
