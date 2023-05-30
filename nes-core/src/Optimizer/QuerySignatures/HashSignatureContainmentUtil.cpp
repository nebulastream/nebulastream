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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/NegateExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/ExpressionToZ3ExprUtil.hpp>
#include <Optimizer/QuerySignatures/HashSignatureContainmentUtil.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/Z3ExprAndFieldMap.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/SlidingWindow.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>

namespace NES::Optimizer {

HashSignatureContainmentUtilPtr HashSignatureContainmentUtil::create(const z3::ContextPtr& context) {
    return std::make_shared<HashSignatureContainmentUtil>(std::move(context));
}

HashSignatureContainmentUtil::HashSignatureContainmentUtil(const z3::ContextPtr& context) {
    this->context = context;
    this->solver = std::make_unique<z3::solver>(*this->context);
}

ContainmentType HashSignatureContainmentUtil::checkContainment(const LogicalOperatorNodePtr& leftOperator,
                                                               const LogicalOperatorNodePtr& rightOperator) {
    NES_TRACE2("Checking for containment.");
    try {
        if (leftOperator->getHashBasedSignature() == rightOperator->getHashBasedSignature()) {
            return ContainmentType::EQUALITY;
        } else if (leftOperator->instanceOf<FilterLogicalOperatorNode>()
                   && rightOperator->instanceOf<FilterLogicalOperatorNode>()) {
            auto leftFilter = leftOperator->as<FilterLogicalOperatorNode>();
            auto rightFilter = rightOperator->as<FilterLogicalOperatorNode>();
            return checkFilterContainment(leftFilter, rightFilter);
        } else if (leftOperator->instanceOf<ProjectionLogicalOperatorNode>()
                   && rightOperator->instanceOf<ProjectionLogicalOperatorNode>()) {
            return checkProjectionContainment(leftOperator, rightOperator);
        } else if (leftOperator->instanceOf<WindowLogicalOperatorNode>()
                   && rightOperator->instanceOf<WindowLogicalOperatorNode>()) {
            auto leftWindow = leftOperator->as<WindowLogicalOperatorNode>();
            auto rightWindow = rightOperator->as<WindowLogicalOperatorNode>();
            if (leftWindow->getWindowDefinition()->getWindowType()->isTimeBasedWindowType()
                == rightWindow->getWindowDefinition()->getWindowType()->isTimeBasedWindowType()) {
                return checkWindowContainment(leftWindow, rightWindow);
            }
        }
    } catch (const std::exception& e) {
        NES_ERROR2("Error while checking containment: {}", e.what());
    }
    return ContainmentType::NO_CONTAINMENT;
}

ContainmentType HashSignatureContainmentUtil::checkFilterContainment(const FilterLogicalOperatorNodePtr& leftOperator,
                                                                     const FilterLogicalOperatorNodePtr& rightOperator) {
    NES_TRACE2("Filter predicate: {}", leftOperator->getPredicate()->toString());
    NES_TRACE2("Filter predicate: {}", rightOperator->getPredicate()->toString());
    auto leftFilterExprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(leftOperator->getPredicate(), context);
    auto rightFilterExprAndFieldMap = ExpressionToZ3ExprUtil::createForExpression(rightOperator->getPredicate(), context);
    NES_TRACE2("Left filter expr: {}", leftFilterExprAndFieldMap->getExpr()->to_string());
    NES_TRACE2("right filter expr: {}", rightFilterExprAndFieldMap->getExpr()->to_string());
    z3::expr_vector leftFOL(*context);
    z3::expr_vector rightFOL(*context);
    leftFOL.push_back(*leftFilterExprAndFieldMap->getExpr());
    rightFOL.push_back(*rightFilterExprAndFieldMap->getExpr());
    if (checkContainmentConditionsUnsatisfied(leftFOL, rightFOL)) {
        return ContainmentType::RIGHT_SIG_CONTAINED;
    } else if (checkContainmentConditionsUnsatisfied(rightFOL, leftFOL)) {
        return ContainmentType::LEFT_SIG_CONTAINED;
    }
    return ContainmentType::NO_CONTAINMENT;
}

ContainmentType HashSignatureContainmentUtil::checkWindowContainment(const WindowLogicalOperatorNodePtr& leftOperator,
                                                                     const WindowLogicalOperatorNodePtr& rightOperator) {
    auto containmentType = ContainmentType::NO_CONTAINMENT;
    if (leftOperator->getWindowDefinition()->isKeyed() == rightOperator->getWindowDefinition()->isKeyed()) {
        for (uint64_t i = 0; i < leftOperator->getWindowDefinition()->getKeys().size(); ++i) {
            if (leftOperator->getWindowDefinition()->getKeys()[i]->equal(rightOperator->getWindowDefinition()->getKeys()[i])) {
                auto leftTimeWindow = leftOperator->getWindowDefinition()->getWindowType()->asTimeBasedWindowType(
                    leftOperator->getWindowDefinition()->getWindowType());
                auto rightTimeWindow = rightOperator->getWindowDefinition()->getWindowType()->asTimeBasedWindowType(
                    rightOperator->getWindowDefinition()->getWindowType());
                if (leftTimeWindow->getTimeCharacteristic()->getField()->isEqual(
                        rightTimeWindow->getTimeCharacteristic()->getField())) {
                    uint64_t leftLength = 0;
                    uint64_t rightLength = 0;
                    uint64_t leftSlide = 0;
                    uint64_t rightSlide = 0;
                    obtainSizeAndSlide(leftTimeWindow, leftLength, leftSlide);
                    obtainSizeAndSlide(rightTimeWindow, rightLength, rightSlide);
                    if (getWindowContainmentRelationship(leftOperator,
                                                         rightOperator,
                                                         leftLength,
                                                         rightLength,
                                                         leftSlide,
                                                         rightSlide)) {
                        containmentType = ContainmentType::RIGHT_SIG_CONTAINED;
                    } else if (getWindowContainmentRelationship(rightOperator,
                                                                leftOperator,
                                                                rightLength,
                                                                leftLength,
                                                                rightSlide,
                                                                leftSlide)) {
                        containmentType = ContainmentType::LEFT_SIG_CONTAINED;
                    }
                }
            }
        }
    }
    return containmentType;
}

bool HashSignatureContainmentUtil::getWindowContainmentRelationship(const WindowLogicalOperatorNodePtr& containerOperator,
                                                                    const WindowLogicalOperatorNodePtr& containeeOperator,
                                                                    uint64_t containerLength,
                                                                    uint64_t containeeLength,
                                                                    uint64_t containerSlide,
                                                                    uint64_t containeeSlide) const {
    if (containerLength <= containeeLength && containerSlide <= containeeSlide && containeeLength % containerLength == 0
        && containeeSlide % containerSlide == 0) {
        if (containerSlide == containeeSlide || (containerSlide == containerLength && containeeLength == containeeSlide)) {
            if (containerOperator->getWindowDefinition()->getWindowAggregation().size()
                == containeeOperator->getWindowDefinition()->getWindowAggregation().size()) {
                if (containerOperator->getOutputSchema()->equals(containeeOperator->getOutputSchema())) {
                    for (const auto& item : containerOperator->getWindowDefinition()->getWindowAggregation()) {
                        if (item->getType() != Windowing::WindowAggregationDescriptor::Type::Avg
                            && item->getType() != Windowing::WindowAggregationDescriptor::Type::Median) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    return false;
}

void HashSignatureContainmentUtil::obtainSizeAndSlide(Windowing::TimeBasedWindowTypePtr& timeWindow,
                                                      uint64_t& length,
                                                      uint64_t& slide) const {
    auto multiplier = timeWindow->getTimeCharacteristic()->getTimeUnit().getMultiplier();
    if (timeWindow->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::TUMBLINGWINDOW) {
        auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(timeWindow);
        length = tumblingWindow->getSize().getTime() * multiplier;
        slide = length;
    } else if (timeWindow->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::SLIDINGWINDOW) {
        auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(timeWindow);
        length = slidingWindow->getSize().getTime() * multiplier;
        slide = slidingWindow->getSlide().getTime() * multiplier;
    } else {
        NES_THROW_RUNTIME_ERROR("QuerySignatureUtil: Unknown window Time Characteristic");
    }
}

ContainmentType HashSignatureContainmentUtil::checkProjectionContainment(const LogicalOperatorNodePtr& leftOperator,
                                                                         const LogicalOperatorNodePtr& rightOperator) {
    bool contained = true;
    if (leftOperator->getOutputSchema()->getSize() >= rightOperator->getOutputSchema()->getSize()) {
        for (size_t i = 0; i < rightOperator->getOutputSchema()->getSize(); ++i) {
            contained = leftOperator->getOutputSchema()->contains(rightOperator->getOutputSchema()->get(i)->getName());
        }
        if (contained) {
            return ContainmentType::RIGHT_SIG_CONTAINED;
        }
    } else if (leftOperator->getOutputSchema()->getSize() <= rightOperator->getOutputSchema()->getSize()) {
        for (size_t i = 0; i < leftOperator->getOutputSchema()->getSize(); ++i) {
            contained = rightOperator->getOutputSchema()->contains(leftOperator->getOutputSchema()->get(i)->getName());
        }
        if (contained) {
            return ContainmentType::LEFT_SIG_CONTAINED;
        }
    }
    return ContainmentType::NO_CONTAINMENT;
}

bool HashSignatureContainmentUtil::checkContainmentConditionsUnsatisfied(z3::expr_vector& negatedCondition,
                                                                         z3::expr_vector& condition) {
    NES_TRACE2("NegatedCondition: {}", negatedCondition.to_string());
    NES_TRACE2("condition: {}", condition.to_string());
    solver->push();
    solver->add(!mk_and(negatedCondition).simplify());
    solver->push();
    solver->add(mk_and(condition).simplify());
    NES_TRACE2("Check unsat: {}", solver->check());
    NES_TRACE2("Content of solver: {}", solver->to_smt2());
    bool conditionUnsatisfied = false;
    if (solver->check() == z3::unsat) {
        conditionUnsatisfied = true;
    }
    solver->pop(NUMBER_OF_CONDITIONS_TO_POP_FROM_SOLVER);
    counter++;
    if (counter >= RESET_SOLVER_THRESHOLD) {
        resetSolver();
    }
    return conditionUnsatisfied;
}

void HashSignatureContainmentUtil::resetSolver() {
    solver->reset();
    counter = 0;
}
}// namespace NES::Optimizer
