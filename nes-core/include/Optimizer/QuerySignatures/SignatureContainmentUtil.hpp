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

enum ContainmentOperator { PROJECTION, WINDOW, FILTER };
enum ContainmentDetected { SQP_CONTAINED, NEW_QUERY_CONTAINED, EQUALITY };

/**
 * @brief This is a utility to compare two signatures
 */
class SignatureContainmentUtil {

  public:
    static SignatureContainmentUtilPtr create(const z3::ContextPtr& context, const z3::ContextPtr& contextSQPContainment);

    explicit SignatureContainmentUtil(const z3::ContextPtr& context, const z3::ContextPtr& contextSQPContainment);

    /**
     * @brief Check equality of the given signatures
     * @param signature1
     * @param signature2
     * @return true if tey are equal else false
     */
    std::map<ContainmentOperator, ContainmentDetected> checkContainment(const QuerySignaturePtr& signature1, const QuerySignaturePtr& signature2);
    /**
     * @brief Reset z3 solver
     * @return true if reset successful else false
     */
    bool resetSolver();

  private:
    z3::ContextPtr context;
    z3::ContextPtr contextSQPContainment;
    z3::SolverPtr solver;
    z3::SolverPtr solverSQPContainment;
    uint64_t counter;
};
}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_SIGNATUREEQUALITYUTIL_HPP_
