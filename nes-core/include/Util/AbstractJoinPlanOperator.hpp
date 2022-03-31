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

#ifndef NES_ABSTRACTJOINPLANOPERATOR_HPP
#define NES_ABSTRACTJOINPLANOPERATOR_HPP

#include <Windowing/JoinEdge.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Util/OptimizerPlanOperator.hpp>
#include <Windowing/JoinForwardRefs.hpp>


namespace NES {

class AbstractJoinPlanOperator;
using AbstractJoinPlanOperatorPtr = std::shared_ptr<AbstractJoinPlanOperator>;

        class AbstractJoinPlanOperator : public OptimizerPlanOperator {

          public:
            /**
     * @brief - constructor for building a OptimizerPlanOperator that is essentially just one logical stream
     */
            AbstractJoinPlanOperator(OptimizerPlanOperatorPtr leftChild,
                                     OptimizerPlanOperatorPtr rightChild,
                                     Join::LogicalJoinDefinitionPtr joinPredicate);

          private:

            /**
	 * The left child.
	 */
            OptimizerPlanOperatorPtr leftChild;

            /**
	 * The right child.
	 */
            OptimizerPlanOperatorPtr rightChild;

            /**
	 * The join predicate applied during this join.
	 */
            Join::LogicalJoinDefinitionPtr joinPredicate;

            /**
	 * An array of tables that are involved in the plans below this join.
	 */
            std::set<OptimizerPlanOperatorPtr> involvedOptimizerPlanOperators;

            /**
	 * The cardinality of the operator.
	 */
            long cardinality;

          public:

            const Join::LogicalJoinDefinitionPtr& getJoinPredicate() const;
            void setJoinPredicate(const Join::LogicalJoinDefinitionPtr joinPredicate);
            AbstractJoinPlanOperator();

            bool operator<(const AbstractJoinPlanOperator& abstractJoinPlanOperator2) const
            {
                return this->getId() < abstractJoinPlanOperator2.getId();
            }

            const OptimizerPlanOperatorPtr& getLeftChild() const;
            void setLeftChild(const OptimizerPlanOperatorPtr& leftChild);
            const OptimizerPlanOperatorPtr& getRightChild() const;
            void setRightChild(const OptimizerPlanOperatorPtr& rightChild);

            const std::set<NES::OptimizerPlanOperatorPtr>& getInvolvedOptimizerPlanOperators1();
            void setInvolvedOptimizerPlanOperators1(const std::set<OptimizerPlanOperatorPtr>& involvedOptimizerPlanOperators);
            long getCardinality1() const;
            void setCardinality1(long cardinality);
        };

    } // namespace NES
#endif//NES_ABSTRACTJOINPLANOPERATOR_HPP
