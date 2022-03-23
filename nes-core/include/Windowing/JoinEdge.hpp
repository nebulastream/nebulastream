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

#ifndef NES_JOINEDGE_HPP
#define NES_JOINEDGE_HPP

#include <Util/OptimizerPlanOperator.hpp>

namespace NES::Join {

        /**
 * @brief Defines left stream, right stream, joinDefinition (to check joining attributes) and optionally selectivity of a join
 */
        //TODO Implement
        // TODO find out what is a heuristic for join selectivity.
        class JoinEdge {

          public:
            JoinEdge(SourceLogicalOperatorNodePtr leftSource, SourceLogicalOperatorNodePtr rightSource, LogicalJoinDefinitionPtr joinDefinition);
            JoinEdge(SourceLogicalOperatorNodePtr leftSource, SourceLogicalOperatorNodePtr rightSource, LogicalJoinDefinitionPtr joinDefinition, float selectivity);
            JoinEdge(OptimizerPlanOperator leftOperator, SourceLogicalOperatorNodePtr rightSource);
            JoinEdge(OptimizerPlanOperator leftOperator, SourceLogicalOperatorNodePtr rightSource, float selectivity);


            void setLeftSource(const SourceLogicalOperatorNodePtr& leftSubPlan);
            const SourceLogicalOperatorNodePtr& getLeftSource() const;
            /*
             * in case the left structure is more complex than just a source (e.g. joined sources)
             */
            const OptimizerPlanOperator& getLeftOperator() const;
            const SourceLogicalOperatorNodePtr& getRightSource() const;
            const LogicalJoinDefinitionPtr& getJoinDefinition() const;
            float getSelectivity() const;



            void setRightSource(const SourceLogicalOperatorNodePtr& rightSubPlan);


            void setJoinDefinition(const LogicalJoinDefinitionPtr& joinDefinition);


            void setSelectivity(float selectivity);

            std::string toString();

            void deriveAndSetLeftOperator();
            void setLeftOperator(const OptimizerPlanOperator& leftOperator);

          private:
            SourceLogicalOperatorNodePtr leftSource;
            OptimizerPlanOperator leftOperator;
            SourceLogicalOperatorNodePtr rightSource;
            LogicalJoinDefinitionPtr joinDefinition;
            float selectivity = 0.5;
        };


        using JoinEdgePtr = std::shared_ptr<JoinEdge>;
    }// namespace NES::Join
#endif//NES_JOINEDGE_HPP
