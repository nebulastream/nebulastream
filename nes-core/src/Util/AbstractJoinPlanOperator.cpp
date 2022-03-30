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

#include <Util/AbstractJoinPlanOperator.hpp>

NES::AbstractJoinPlanOperator::AbstractJoinPlanOperator(const OptimizerPlanOperator &leftChild,
                                                        const OptimizerPlanOperator &rightChild,
                                                        Join::LogicalJoinDefinitionPtr joinPredicate){
        this->leftChild = leftChild;
        this->rightChild = rightChild;
        this->joinPredicate = joinPredicate;
    }

    const NES::OptimizerPlanOperator& NES::AbstractJoinPlanOperator::getLeftChild() const { return leftChild; }
    void NES::AbstractJoinPlanOperator::setLeftChild(const NES::OptimizerPlanOperator& leftChild) {
        AbstractJoinPlanOperator::leftChild = leftChild;
    }
    const NES::OptimizerPlanOperator& NES::AbstractJoinPlanOperator::getRightChild() const { return rightChild; }
    void NES::AbstractJoinPlanOperator::setRightChild(const NES::OptimizerPlanOperator& rightChild) {
        AbstractJoinPlanOperator::rightChild = rightChild;
    }
    const NES::Join::LogicalJoinDefinitionPtr& NES::AbstractJoinPlanOperator::getJoinPredicate() const { return joinPredicate; }
    void NES::AbstractJoinPlanOperator::setJoinPredicate(const Join::LogicalJoinDefinitionPtr joinPredicate) {
        AbstractJoinPlanOperator::joinPredicate = joinPredicate;
    }


    const std::set<NES::OptimizerPlanOperator> NES::AbstractJoinPlanOperator::getInvolvedOptimizerPlanOperators() {
        // in case this is empty yet, set it by checking the involved streams of left and right child.
        if (involvedOptimizerPlanOperators.size() == 0){
            std::set<OptimizerPlanOperator> leftTabs = leftChild.getInvolvedOptimizerPlanOperators();
            std::set<OptimizerPlanOperator> rightTabs = rightChild.getInvolvedOptimizerPlanOperators();
            std::set<OptimizerPlanOperator> thisTabs;
            std::set_union(leftTabs.begin(), leftTabs.end(),
                           rightTabs.begin(), rightTabs.end(),
                           std::inserter(thisTabs, thisTabs.begin()));
            setInvolvedOptimizerPlanOperators1(thisTabs);
            return thisTabs;
        }
        else {
            return involvedOptimizerPlanOperators;
        }

    }

    long NES::AbstractJoinPlanOperator::getCardinality1() const { return cardinality; }
    void NES::AbstractJoinPlanOperator::setCardinality1(long cardinality) { AbstractJoinPlanOperator::cardinality = cardinality; }
    void NES::AbstractJoinPlanOperator::setInvolvedOptimizerPlanOperators1(
        const std::set<OptimizerPlanOperator>& involvedOptimizerPlanOperators) {
        AbstractJoinPlanOperator::involvedOptimizerPlanOperators = involvedOptimizerPlanOperators;
    }
