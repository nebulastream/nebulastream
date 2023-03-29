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

#include <memory>
#include <z3++.h>

namespace z3 {
class solver;
using SolverPtr = std::shared_ptr<solver>;

class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

class SignatureContainmentUtil;
using SignatureContainmentUtilPtr = std::shared_ptr<SignatureContainmentUtil>;
/**
 * @brief enum describing the given containment relationship
 */
enum class ContainmentType : uint8_t { NO_CONTAINMENT, LEFT_SIG_CONTAINED, RIGHT_SIG_CONTAINED, EQUALITY };

/**
 * @brief This is a utility to compare two signatures
 */
class SignatureContainmentUtil {

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
    explicit SignatureContainmentUtil(const z3::ContextPtr& context);

    /**
     * @brief Check containment relationships for the given signatures as follows
     * check if right sig ⊆ left sig for projections (including map conditions)
     *      true: check left sig ⊆ right sig for projections (including map conditions) --> if true, we have equal projections, i.e. can only check other containment relationships if sources, projections, and maps are equal
     *          true: check filter containment --> see checkFilterContainment() for details
     *          false: return RIGHT_SIG_CONTAINED
     *      false: check left sig ⊆ right sig for projections (including map conditions)
     *          true: return LEFT_SIG_CONTAINED
     *          false: return NO_CONTAINMENT
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkContainment(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

  private:
    /**
     * @brief creates conditions for checking projection containment:
     * if we are given a map value for the attribute, we create a FOL as attributeStringName == mapCondition, e.g. age == 25
     * else we indicate that the attribute is involved in the projection as attributeStingName == true
     * all FOL are added to the projectionCondition vector
     * @param signature Query signature to extract conditions from
     * @param projectionCondition z3 expression vector to add conditions to
     */
    void createProjectionCondition(const QuerySignaturePtr& signature, z3::expr_vector& projectionCondition);

    /**
     * @brief check for filter containment as follows:
     * check if right sig ⊆ left sig for filters
     *      true: check if left sig ⊆ right sig
     *          true: return EQUALITY
     *          false: return RIGHT_SIG_CONTAINED
     *      false: check if left sig ⊆ right sig
     *          true: return LEFT_SIG_CONTAINED
     *          false: return NO_CONTAINMENT
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkFilterContainment(const QuerySignaturePtr& leftSignature, const QuerySignaturePtr& rightSignature);

    /**
     * @brief checks if the combination (combined via &&) of negated conditions and non negated conditions is unsatisfiable
     * it also pops the given number of conditions and calls resetSolver() if the counter hits the threshold for resetting
     * @param negatedCondition condition that will be negated
     * @param condition condition that will just be added to the solver as it is
     * @return true if the combination of the given conditions is unsatisfiable, false otherwise
     */
    bool conditionsUnsatisfied(const z3::expr_vector& negatedCondition, const z3::expr_vector& condition);

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
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATUREEQUALITYUTIL_HPP_
