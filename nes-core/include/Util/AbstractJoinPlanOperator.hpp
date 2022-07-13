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

//#include <Windowing/JoinEdge.hpp>

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Util/OptimizerPlanOperator.hpp>
#include <Windowing/JoinForwardRefs.hpp>


namespace NES {

class AbstractJoinPlanOperator;
using AbstractJoinPlanOperatorPtr = std::shared_ptr<AbstractJoinPlanOperator>;

typedef long double;
class AbstractJoinPlanOperator : public OptimizerPlanOperator {

          public:
            /**
     * @brief - constructor for building a AbstractJoinPlanOperator that is a join between an OptimizerPlanOperator
     */
            AbstractJoinPlanOperator(AbstractJoinPlanOperatorPtr leftChild,
                                     AbstractJoinPlanOperatorPtr rightChild,
                                     Join::LogicalJoinDefinitionPtr joinPredicate,
                                     float selectivity,
                                     ExpressionNodePtr filterPredicate = nullptr,
                                     int numberOfPredicates = 1);

            /**
             * @brief - constructor for building an AbstractJoinPlanOperator from a source (OptimizerPlanOperator
             */
            AbstractJoinPlanOperator(OptimizerPlanOperatorPtr source);

          private:
            AbstractJoinPlanOperator(OptimizerPlanOperator source);
            /**
	 * The left child.
	 */
            AbstractJoinPlanOperatorPtr leftChild;

            /**
	 * The right child.
	 */
            AbstractJoinPlanOperatorPtr rightChild;

            /**
	 * The join predicate applied during this join.
	 */
            Join::LogicalJoinDefinitionPtr joinPredicate;

            /**
             * The selectivity of a join between left and rightChild.
             */
            float selectivity;

          public:
            const ExpressionNodePtr& getPredicate() const;
            void setPredicate(const ExpressionNodePtr& predicate);

          private:
            /**
      * Cost issued by this operator
      * This cost is the CPU cost of three things:
             * 1. Access cost of input tuples C_i
             * 2. Predicate evaluation cost (proportional with factor 0.25) to C_i, and multiplied by the number of multi-class predicates
             * 3. Output tuple generation cost C_o
      */
            long double operatorCosts;

            /**
      * Cost of the whole subplan cumulated.
      * OP cost of root + children's cumulative costs
      */
            long double cumulativeCosts;

            /**
      * source cardinality, i.e. size of tuples in window
      * or if joined the output cardinality of this window join.
      */
            long double cardinality;

            /**
             * Defines the attached filter predicate in case of a sequence operator.
             * If the AJPO is only an extended source, it can be a nullptr
             */
            ExpressionNodePtr predicate;

            /**
             * In some cases, joins/sequences apply to multiple number of predicates at the same time.
             * For these cases, the cost of evaluating the predicate is higher
             * This int reflects the number predicates for this join operation
             */
            int numberOfPredicates;

          public:
            int getNumberOfPredicates() const;
            void setNumberOfPredicates(int numberOfPredicates);

          public:
            float getSelectivity() const;
            void setSelectivity(float selectivity);

          private:
            /**
	 * An array of tables that are involved in the plans below this join.
	 */
            std::set<OptimizerPlanOperatorPtr> involvedOptimizerPlanOperators;


          public:

            const Join::LogicalJoinDefinitionPtr& getJoinPredicate() const;
            void setJoinPredicate(const Join::LogicalJoinDefinitionPtr joinPredicate);
            AbstractJoinPlanOperator();

            bool operator<(const AbstractJoinPlanOperator& abstractJoinPlanOperator2) const
            {
                return this->getId() < abstractJoinPlanOperator2.getId();
            }

            const AbstractJoinPlanOperatorPtr& getLeftChild() const;
            void setLeftChild(const AbstractJoinPlanOperatorPtr& leftChild);
            const AbstractJoinPlanOperatorPtr& getRightChild() const;
            void setRightChild(const AbstractJoinPlanOperatorPtr& rightChild);

            const std::set<NES::OptimizerPlanOperatorPtr>& getInvolvedOptimizerPlanOperators();
            void setInvolvedOptimizerPlanOperators(const std::set<OptimizerPlanOperatorPtr>& involvedOptimizerPlanOperators);
            long double getCardinality() const;
            void setCardinality(long double cardinality);
            long double getOperatorCosts() const;
            void setOperatorCosts(long double operatorCosts);
            long double getCumulativeCosts() const;
            void setCumulativeCosts(long double cumulativeCosts);
        };

    } // namespace NES
#endif//NES_ABSTRACTJOINPLANOPERATOR_HPP
