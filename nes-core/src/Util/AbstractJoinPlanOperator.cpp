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

NES::AbstractJoinPlanOperator::AbstractJoinPlanOperator(OptimizerPlanOperatorPtr leftChild,
                                                        OptimizerPlanOperatorPtr rightChild,
                                                        Join::LogicalJoinDefinitionPtr joinPredicate,
                                                        float selectivity){
        this->leftChild = leftChild;
        this->rightChild = rightChild;
        this->joinPredicate = joinPredicate;
        this->selectivity = selectivity;
    }


    const NES::Join::LogicalJoinDefinitionPtr& NES::AbstractJoinPlanOperator::getJoinPredicate() const { return joinPredicate; }
    void NES::AbstractJoinPlanOperator::setJoinPredicate(const Join::LogicalJoinDefinitionPtr joinPredicate) {
        AbstractJoinPlanOperator::joinPredicate = joinPredicate;
    }



    const std::set<NES::OptimizerPlanOperatorPtr>& NES::AbstractJoinPlanOperator::getInvolvedOptimizerPlanOperators1() {
        // in case this is empty yet, set it by checking the involved streams of left and right child.
        if (involvedOptimizerPlanOperators.size() == 0){
            std::set<OptimizerPlanOperatorPtr> leftTabs = leftChild->getInvolvedOptimizerPlanOperators();
            std::set<OptimizerPlanOperatorPtr> rightTabs = rightChild->getInvolvedOptimizerPlanOperators();
            // JVS Declaring static seems to be the issue here
            std::set<OptimizerPlanOperatorPtr> thisTabs;
            std::set_union(leftTabs.begin(), leftTabs.end(),
                           rightTabs.begin(), rightTabs.end(),
                           std::inserter(thisTabs, thisTabs.begin()));

            setInvolvedOptimizerPlanOperators1(thisTabs);
        }

            return involvedOptimizerPlanOperators;

    }
    void NES::AbstractJoinPlanOperator::setInvolvedOptimizerPlanOperators1(
        const std::set<OptimizerPlanOperatorPtr>& involvedOptimizerPlanOperators) {
        AbstractJoinPlanOperator::involvedOptimizerPlanOperators = involvedOptimizerPlanOperators;
    }
    long NES::AbstractJoinPlanOperator::getCardinality1() const { return cardinality; }
    void NES::AbstractJoinPlanOperator::setCardinality1(long cardinality) { AbstractJoinPlanOperator::cardinality = cardinality; }
    const NES::OptimizerPlanOperatorPtr& NES::AbstractJoinPlanOperator::getLeftChild() const { return leftChild; }
    void NES::AbstractJoinPlanOperator::setLeftChild(const NES::OptimizerPlanOperatorPtr& leftChild) {
        AbstractJoinPlanOperator::leftChild = leftChild;
    }
    const NES::OptimizerPlanOperatorPtr& NES::AbstractJoinPlanOperator::getRightChild() const { return rightChild; }
    void NES::AbstractJoinPlanOperator::setRightChild(const NES::OptimizerPlanOperatorPtr& rightChild) {
        AbstractJoinPlanOperator::rightChild = rightChild;
    }
    float NES::AbstractJoinPlanOperator::getSelectivity() const { return selectivity; }
    void NES::AbstractJoinPlanOperator::setSelectivity(float selectivity) { AbstractJoinPlanOperator::selectivity = selectivity; }
