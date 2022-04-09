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

JoinEdge::JoinEdge(AbstractJoinPlanOperatorPtr leftOperator,
                   AbstractJoinPlanOperatorPtr rightOperator, LogicalJoinDefinitionPtr joinDefinition){
        setLeftOperator(leftOperator);
        setRightOperator(rightOperator);
        setJoinDefinition(joinDefinition);
        // JVS: Check the real heuristic here
        setSelectivity(1);
    }
    JoinEdge::JoinEdge(AbstractJoinPlanOperatorPtr leftOperator,
                       AbstractJoinPlanOperatorPtr rightOperator, LogicalJoinDefinitionPtr joinDefinition, float selectivity){
        setLeftOperator(leftOperator);
        setRightOperator(rightOperator);
        setJoinDefinition(joinDefinition);
        setSelectivity(selectivity);
    }

    void JoinEdge::setJoinDefinition(const LogicalJoinDefinitionPtr& joinDefinition) { JoinEdge::joinDefinition = joinDefinition; }

    void JoinEdge::setSelectivity(float selectivity) { JoinEdge::selectivity = selectivity; }

    std::string JoinEdge::toString(){
        std::stringstream ss;
        // Just printing the sources that are to be joined, which keys and a proposed selectivity.
        // if selectivity == default ==> make sure to communicate this.

        ss << "Join Edge between: " << leftOperator->getSourceNode()->toString() << " (left) and " << rightOperator->getSourceNode()->toString() << " (right)";
        ss << "\n Joining on keys: " << joinDefinition->getLeftJoinKey()->getFieldName() << " (left-key) and " << joinDefinition->getRightJoinKey()->getFieldName() << "(right-key)" ;

        // JVS this has to be done at some point more carefully. What is really the default selectivity?
        if (selectivity == 0.5){
            ss << "\n Join with an expected default selectivity of 1 NOTE: This is a heuristic.";
        } else {
            ss << "\n Join with a predicted selectivity of " << selectivity;
        }

        return ss.str();
    }

    const LogicalJoinDefinitionPtr& JoinEdge::getJoinDefinition() const { return joinDefinition; }
    float JoinEdge::getSelectivity() const { return selectivity; }
    const AbstractJoinPlanOperatorPtr& JoinEdge::getLeftOperator() const { return leftOperator; }
    void JoinEdge::setLeftOperator(const AbstractJoinPlanOperatorPtr& leftOperator) { JoinEdge::leftOperator = leftOperator; }
    const AbstractJoinPlanOperatorPtr& JoinEdge::getRightOperator() const { return rightOperator; }

    void JoinEdge::setRightOperator(const AbstractJoinPlanOperatorPtr& rightOperator) { JoinEdge::rightOperator = rightOperator; }


    };// namespace NES::Join
