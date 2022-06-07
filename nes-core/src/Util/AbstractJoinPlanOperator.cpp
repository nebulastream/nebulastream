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
#include <Optimizer/QueryRewrite/JoinOrderOptimizationRule.hpp>

NES::AbstractJoinPlanOperator::AbstractJoinPlanOperator(AbstractJoinPlanOperatorPtr leftChild,
                                                        AbstractJoinPlanOperatorPtr rightChild,
                                                        Join::LogicalJoinDefinitionPtr joinPredicate,
                                                        float selectivity){
        this->leftChild = leftChild;
        this->rightChild = rightChild;
        this->joinPredicate = joinPredicate;
        this->selectivity = selectivity;
    }

NES::AbstractJoinPlanOperator::AbstractJoinPlanOperator(OptimizerPlanOperatorPtr source){
    this->leftChild = nullptr;
    this->rightChild = nullptr;
    this->joinPredicate = nullptr;
    this->selectivity = 1;
    this->setSourceNode(source->getSourceNode());
    this->setId(source->getId());
    this->setCardinality(Optimizer::JoinOrderOptimizationRule::getHardCodedCardinalitiesForSource(source)); // JVS TRY THIS OUT (and line below)
    this->setCumulativeCosts(this->getCardinality());
    this->setOperatorCosts(0);
    std::set<OptimizerPlanOperatorPtr> involvedOptimizerPlanOperators;
    involvedOptimizerPlanOperators.insert(source);
    this->setInvolvedOptimizerPlanOperators(involvedOptimizerPlanOperators);
}

    const NES::Join::LogicalJoinDefinitionPtr& NES::AbstractJoinPlanOperator::getJoinPredicate() const { return joinPredicate; }
    void NES::AbstractJoinPlanOperator::setJoinPredicate(const Join::LogicalJoinDefinitionPtr joinPredicate) {
        AbstractJoinPlanOperator::joinPredicate = joinPredicate;
    }



    const std::set<NES::OptimizerPlanOperatorPtr>& NES::AbstractJoinPlanOperator::getInvolvedOptimizerPlanOperators() {
        // in case this is empty yet, set it by checking the involved streams of left and right child.
        // JVS mind that this is set to == 1 if we are dealing with a base source (check constructor with source)
        if (involvedOptimizerPlanOperators.size() == 0){

            // get left and right tabs
            std::set<OptimizerPlanOperatorPtr> leftTabs;
            std::set<OptimizerPlanOperatorPtr> rightTabs;
            if (leftChild != nullptr){
               leftTabs = leftChild->getInvolvedOptimizerPlanOperators();
            }
            if (rightChild != nullptr){
                rightTabs = rightChild->getInvolvedOptimizerPlanOperators();
            }

            std::set<OptimizerPlanOperatorPtr> thisTabs;
            // JVS Declaring static seems to be the issue here
            std::set_union(leftTabs.begin(), leftTabs.end(),
                           rightTabs.begin(), rightTabs.end(),
                           std::inserter(thisTabs, thisTabs.begin()));

            setInvolvedOptimizerPlanOperators(thisTabs);
        }

            return involvedOptimizerPlanOperators;

    }
    void NES::AbstractJoinPlanOperator::setInvolvedOptimizerPlanOperators(
        const std::set<OptimizerPlanOperatorPtr>& involvedOptimizerPlanOperators) {
        AbstractJoinPlanOperator::involvedOptimizerPlanOperators = involvedOptimizerPlanOperators;
    }
    long double NES::AbstractJoinPlanOperator::getCardinality() const { return cardinality; }
    void NES::AbstractJoinPlanOperator::setCardinality(long double cardinality) { AbstractJoinPlanOperator::cardinality = cardinality; }

    float NES::AbstractJoinPlanOperator::getSelectivity() const { return selectivity; }
    void NES::AbstractJoinPlanOperator::setSelectivity(float selectivity) { AbstractJoinPlanOperator::selectivity = selectivity; }
    long double NES::AbstractJoinPlanOperator::getOperatorCosts() const { return operatorCosts; }
    void NES::AbstractJoinPlanOperator::setOperatorCosts(long double operatorCosts) {
        AbstractJoinPlanOperator::operatorCosts = operatorCosts;
    }
    long double NES::AbstractJoinPlanOperator::getCumulativeCosts() const { return cumulativeCosts; }
    void NES::AbstractJoinPlanOperator::setCumulativeCosts(long double cumulativeCosts) {
        AbstractJoinPlanOperator::cumulativeCosts = cumulativeCosts;
    }
    const NES::AbstractJoinPlanOperatorPtr& NES::AbstractJoinPlanOperator::getLeftChild() const { return leftChild; }
    void NES::AbstractJoinPlanOperator::setLeftChild(const NES::AbstractJoinPlanOperatorPtr& leftChild) {
        AbstractJoinPlanOperator::leftChild = leftChild;
    }
    const NES::AbstractJoinPlanOperatorPtr& NES::AbstractJoinPlanOperator::getRightChild() const { return rightChild; }
    void NES::AbstractJoinPlanOperator::setRightChild(const NES::AbstractJoinPlanOperatorPtr& rightChild) {
        AbstractJoinPlanOperator::rightChild = rightChild;
    }
