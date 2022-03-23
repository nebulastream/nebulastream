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

#include "Util/OptimizerPlanOperator.hpp"

// set all values to default
/*NES::OptimizerPlanOperator::OptimizerPlanOperator(SourceLogicalOperatorNode source) {
    setOperatorCosts(-1);
    setCumulativeCosts(0);
    setCardinality(0); // can i deduce this somehow?
    setId(0); // see if this can be done automatically
    setSourceNode(source);
    //setInvolvedOptimizerPlanOperators(nullptr);
}*/

    // set all values to default
int NES::OptimizerPlanOperator::current_id = 0;


    NES::OptimizerPlanOperator::OptimizerPlanOperator(SourceLogicalOperatorNodePtr source) {
            setOperatorCosts(0);
            setCumulativeCosts(0);
            setCardinality(0); // can i deduce this somehow?
            setSourceNode(source);
            id = ++OptimizerPlanOperator::current_id;
            //setInvolvedOptimizerPlanOperators(nullptr);
        }

    NES::OptimizerPlanOperator::OptimizerPlanOperator() {
        setOperatorCosts(-1);
        setCumulativeCosts(0);
        setCardinality(0);
        id = ++OptimizerPlanOperator::current_id;
    }


    long NES::OptimizerPlanOperator::getOperatorCosts() const { return operatorCosts; }
    void NES::OptimizerPlanOperator::setOperatorCosts(long operatorCosts) { OptimizerPlanOperator::operatorCosts = operatorCosts; }
    long NES::OptimizerPlanOperator::getCumulativeCosts() const { return cumulativeCosts; }
    void NES::OptimizerPlanOperator::setCumulativeCosts(long cumulativeCosts) {
        OptimizerPlanOperator::cumulativeCosts = cumulativeCosts;
    }
    long NES::OptimizerPlanOperator::getCardinality() const { return cardinality; }
    void NES::OptimizerPlanOperator::setCardinality(long cardinality) { OptimizerPlanOperator::cardinality = cardinality; }
    int NES::OptimizerPlanOperator::getId() const { return id; }
    const std::set<NES::OptimizerPlanOperator>& NES::OptimizerPlanOperator::getInvolvedOptimizerPlanOperators() const {
        return involvedOptimizerPlanOperators;
    }
    void NES::OptimizerPlanOperator::setInvolvedOptimizerPlanOperators(
        const std::set<OptimizerPlanOperator>& involvedOptimizerPlanOperators) {
        OptimizerPlanOperator::involvedOptimizerPlanOperators = involvedOptimizerPlanOperators;
    }
    const NES::SourceLogicalOperatorNodePtr& NES::OptimizerPlanOperator::getSourceNode() const { return sourceNode; }
    void NES::OptimizerPlanOperator::setSourceNode(const NES::SourceLogicalOperatorNodePtr& sourceNode) {
        OptimizerPlanOperator::sourceNode = sourceNode;
    }
