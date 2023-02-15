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
enum ContainmentDetected { NO_CONTAINMENT, SIG_ONE_CONTAINED, SIG_TWO_CONTAINED, EQUALITY };

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
     * check if sig2 ⊆ sig1 for projections (including map conditions)
     *      true: check sig1 ⊆ sig2 for projections (including map conditions) --> if true, we have equal projections, i.e. can only check other containment relationships if sources, projections, and maps are equal
     *          true: check filter containment --> see checkFilterContainment() for details
     *          false: return SIG_TWO_CONTAINED
     *      false: check sig1 ⊆ sig2 for projections (including map conditions)
     *          true: return SIG_ONE_CONTAINED
     *          false: return NO_CONTAINMENT
     * @param signature1
     * @param signature2
     * @return enum with containment relationships
     */
    ContainmentDetected checkContainment(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2);

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
     * check if sig2 ⊆ sig1 for filters
     *      true: check if sig1 ⊆ sig2
     *          true: return EQUALITY
     *          false: return SIG_TWO_CONTAINED
     *      false: check if sig1 ⊆ sig2
     *          true: return SIG_ONE_CONTAINED
     *          false: return NO_CONTAINMENT
     * @param signature1
     * @param signature2
     * @return
     */
    ContainmentDetected checkFilterContainment(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2);

    /**
     * @brief pop expressions from z3 solver and reset if number of popped expressions exceeds 20050
     * @param numberOfValuesToPop how many expressions should be popped from the solver
     * @return true if solver was reset, otherwise false
     */
    bool resetSolver(uint8_t numberOfValuesToPop);

    z3::ContextPtr context;
    z3::SolverPtr solver;
    uint64_t counter;
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATUREEQUALITYUTIL_HPP_
