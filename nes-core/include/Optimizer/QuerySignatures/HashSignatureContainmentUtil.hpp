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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_HASHSIGNATURECONTAINMENTUTIL_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYSIGNATURES_HASHSIGNATURECONTAINMENTUTIL_HPP_

#include <Optimizer/QuerySignatures/Z3SignatureContainmentUtil.hpp>
#include <memory>

namespace z3 {
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {
class LogicalOperatorNode;
using LogicalOperatorNodePtr = std::shared_ptr<LogicalOperatorNode>;
}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

class HashSignatureContainmentUtil;
using HashSignatureContainmentUtilPtr = std::shared_ptr<HashSignatureContainmentUtil>;

/**
 * @brief This is a utility to compare two signatures
 */
class HashSignatureContainmentUtil {

  public:
    /**
     * @brief creates an instance of the HashSignatureContainmentUtil
     * @return instance of HashSignatureContainmentUtil
     */
    static HashSignatureContainmentUtilPtr create(const z3::ContextPtr& context);

    /**
     * @brief constructor for signatureContainmentUtil
     * @param context The Z3 context for the SMT solver
     */
    explicit HashSignatureContainmentUtil(const z3::ContextPtr& context);

    /**
     * @brief Check containment relationships for the given signatures as follows
     * check if right sig ⊆ left sig for projections (including map conditions)
     *      First check for WindowContainment
     *      In case of window equality, we continue to check for projection containment
     *      In case of projection equality, we finally check for filter containment
     * @param leftSignature
     * @param rightSignature
     * @return enum with containment relationships
     */
    ContainmentType checkContainment(const LogicalOperatorNodePtr& leftOperator, const LogicalOperatorNodePtr& rightOperator);

  private:
    /**
     * @brief Checks the containment relationship between two filter operations
     * to do so, we employ an SMT solver to check if the left filter is contained in the right filter
     * otherwise, we check if the right filter is contained in the left filter
     * @param leftOperator left filter operation
     * @param rightOperator right filter operation
     * @return containment relationship between the two filters
     */
    ContainmentType checkFilterContainment(const FilterLogicalOperatorNodePtr& leftOperator,
                                           const FilterLogicalOperatorNodePtr& rightOperator);
    /**
     * @brief Checks the containment relationship between two projection operations
     * to do so, we simply iterate over the left projection's map conditions and check if they are contained in the right projection
     * if that is not the case, we do it the other way around, as well
     * @param leftOperator left projection operation
     * @param rightOperator right projection operation
     * @return containment relationship between the two projection operations
     */
    ContainmentType checkProjectionContainment(const LogicalOperatorNodePtr& leftOperator,
                                               const LogicalOperatorNodePtr& rightOperator);
    /**
     * @brief Checks the containment relationship between two window operations
     * @param leftOperator left window operation
     * @param rightOperator right window operation
     * @return containment relationship between the two windows
     */
    ContainmentType checkWindowContainment(const WindowLogicalOperatorNodePtr& leftOperator,
                                           const WindowLogicalOperatorNodePtr& rightOperator);

    /**
     * @brief Check if the given window is a subset of the other window
     * @param containerOperator container window operation
     * @param containeeOperator contained window operation
     * @param containerLength container window size
     * @param containeeLength containee window size
     * @param containerSlide container window slide
     * @param containeeSlide containee window slide
     * @return
     */
    bool getWindowContainmentRelationship(const WindowLogicalOperatorNodePtr& containerOperator,
                                          const WindowLogicalOperatorNodePtr& containeeOperator,
                                          uint64_t containerLength,
                                          uint64_t containeeLength,
                                          uint64_t containerSlide,
                                          uint64_t containeeSlide) const;
    /**
     * @brief Obtain the size and slide of the given time window
     * @param timeWindow time window we want to obtain size and slide for
     * @param length variable to save the window size in
     * @param slide variable to save the window slide in
     */
    void obtainSizeAndSlide(Windowing::TimeBasedWindowTypePtr& timeWindow, uint64_t& length, uint64_t& slide) const;

    /**
     * @brief checks if the combination (combined via &&) of negated conditions and non negated conditions is unsatisfiable
     * it also pops the given number of conditions and calls resetSolver() if the counter hits the threshold for resetting
     * @param negatedCondition condition that will be negated
     * @param condition condition that will just be added to the solver as it is
     * @return true if the combination of the given conditions is unsatisfiable, false otherwise
     */
    bool checkContainmentConditionsUnsatisfied(z3::expr_vector& negatedCondition, z3::expr_vector& condition);

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