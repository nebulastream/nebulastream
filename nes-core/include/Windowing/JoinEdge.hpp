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
        class JoinEdge {

          public:
            JoinEdge(OptimizerPlanOperatorPtr leftOperator, OptimizerPlanOperatorPtr rightSource, LogicalJoinDefinitionPtr joinDefinition);
            JoinEdge(OptimizerPlanOperatorPtr leftOperator, OptimizerPlanOperatorPtr rightSource, LogicalJoinDefinitionPtr joinDefinition ,float selectivity);


            const LogicalJoinDefinitionPtr& getJoinDefinition() const;
            float getSelectivity() const;


            void setJoinDefinition(const LogicalJoinDefinitionPtr& joinDefinition);


            void setSelectivity(float selectivity);

            std::string toString();

            const OptimizerPlanOperatorPtr& getLeftOperator() const;
            void setLeftOperator(const OptimizerPlanOperatorPtr& leftOperator);
            const OptimizerPlanOperatorPtr& getRightOperator() const;
            void setRightOperator(const OptimizerPlanOperatorPtr& rightOperator);

          private:
            OptimizerPlanOperatorPtr leftOperator;
            OptimizerPlanOperatorPtr rightOperator;
            LogicalJoinDefinitionPtr joinDefinition;
            float selectivity = 1;
        };


        using JoinEdgePtr = std::shared_ptr<JoinEdge>;
    }// namespace NES::Join
#endif//NES_JOINEDGE_HPP
