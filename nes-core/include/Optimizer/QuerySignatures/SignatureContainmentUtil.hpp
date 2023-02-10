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
     * @brief constructor fot signatureContainmentUtil
     * @param context The Z3 context for the SMT solver
     */
    explicit SignatureContainmentUtil(const z3::ContextPtr& context);

    /**
     * @brief Check containment relationships for the given signatures
     * @param signature1
     * @param signature2
     * @return enum with containment relationships
     */
    ContainmentDetected checkContainment(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2);
    /**
     * @brief Reset z3 solver
     * @return true if reset successful else false
     */
    bool resetSolver();

  private:
    z3::ContextPtr context;
    z3::SolverPtr solver;
    uint64_t counter;
    void createProjectionCondition(const QuerySignaturePtr& signature1, z3::expr_vector& projectionCondition);
    ContainmentDetected checkFilterContainment(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2);
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATUREEQUALITYUTIL_HPP_
