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

#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/JoinEdge.hpp>
#include <sstream>

namespace NES::Join {
// TODO: Setting Cardinality somehow.
JoinEdge::JoinEdge(SourceLogicalOperatorNodePtr leftSource, SourceLogicalOperatorNodePtr rightSource, LogicalJoinDefinitionPtr joinDefinition) : joinDefinition(joinDefinition){
    setLeftSource(leftSource);
    setRightSource(rightSource);
    setJoinDefinition(joinDefinition);
    // TODO: Check the real heuristic here
    setSelectivity(0.5);
    deriveAndSetLeftOperator();
    }

    JoinEdge::JoinEdge(SourceLogicalOperatorNodePtr leftSource, SourceLogicalOperatorNodePtr rightSource, LogicalJoinDefinitionPtr joinDefinition, float selectivity): joinDefinition(joinDefinition){
        setLeftSource(leftSource);
        setRightSource(rightSource);
        setJoinDefinition(joinDefinition);
        setSelectivity(selectivity);
        deriveAndSetLeftOperator();
    }

    JoinEdge::JoinEdge(OptimizerPlanOperator leftOperator, SourceLogicalOperatorNodePtr rightSource){
        setLeftOperator(leftOperator);
        setRightSource(rightSource);
        setJoinDefinition(joinDefinition);
        // TODO: Check the real heuristic here
        setSelectivity(0.5);
    }
    JoinEdge::JoinEdge(OptimizerPlanOperator leftOperator, SourceLogicalOperatorNodePtr rightSource, float selectivity){
        setLeftOperator(leftOperator);
        setRightSource(rightSource);
        setJoinDefinition(joinDefinition);
        setSelectivity(selectivity);
    }



    void JoinEdge::setLeftSource(const SourceLogicalOperatorNodePtr& leftSource) { JoinEdge::leftSource = leftSource; }

    void JoinEdge::setRightSource(const SourceLogicalOperatorNodePtr& rightSource) { JoinEdge::rightSource = rightSource; }

    void JoinEdge::setJoinDefinition(const LogicalJoinDefinitionPtr& joinDefinition) { JoinEdge::joinDefinition = joinDefinition; }

    void JoinEdge::setSelectivity(float selectivity) { JoinEdge::selectivity = selectivity; }

    std::string JoinEdge::toString(){
        std::stringstream ss;
        // Just printing the sources that are to be joined, which keys and a proposed selectivity.
        // if selectivity == default ==> make sure to communicate this.

        ss << "Join Edge between: " << leftSource->toString() << " (left) and " << rightSource->toString() << " (right)";
        ss << "\n Joining on keys: " << joinDefinition->getLeftJoinKey()->getFieldName() << " (left-key) and " << joinDefinition->getRightJoinKey()->getFieldName() << "(right-key)" ;

        // TODO this has to be done at some point more carefully. What is really the default selectivity?
        if (selectivity == 0.5){
            ss << "\n Join with an expected default selectivity of 0.5 NOTE: This is a heuristic.";
        } else {
            ss << "\n Join with a predicted selectivity of " << selectivity;
        }

        return ss.str();
    }

    // TODO implement ID generator.
    void JoinEdge::deriveAndSetLeftOperator(){
        auto leftOperator = OptimizerPlanOperator(leftSource);
        leftOperator.setOperatorCosts(0);
        leftOperator.setCumulativeCosts(leftOperator.getCardinality());
    }
    const SourceLogicalOperatorNodePtr& JoinEdge::getLeftSource() const { return leftSource; }
    const OptimizerPlanOperator& JoinEdge::getLeftOperator() const { return leftOperator; }
    const SourceLogicalOperatorNodePtr& JoinEdge::getRightSource() const { return rightSource; }
    const LogicalJoinDefinitionPtr& JoinEdge::getJoinDefinition() const { return joinDefinition; }
    float JoinEdge::getSelectivity() const { return selectivity; }
    void JoinEdge::setLeftOperator(const OptimizerPlanOperator& leftOperator) { JoinEdge::leftOperator = leftOperator; }

    };// namespace NES::Join
