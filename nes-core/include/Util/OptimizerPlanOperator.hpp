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

#ifndef NES_OPTIMIZERPLANOPERATOR_HPP
#define NES_OPTIMIZERPLANOPERATOR_HPP

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

/*
 * Resembles one entry in the table produced in dynamic programming to estimate the best join order.
 * This is an intermediate representation of an operator in a join table
 * an operator could be either a source (logical stream) or a combination of multiple sources (i.e. an intermediate product after one or more joins)
 * Examples: 1. Just source R or 2. R |><| S
 */
namespace NES {

class OptimizerPlanOperator {

  public:
    /**
     * @brief - constructor for building a OptimizerPlanOperator that is essentially just one logical stream
     */
    OptimizerPlanOperator(SourceLogicalOperatorNodePtr source);

    /**
     * @brief - constructor for building a OptimizerPlanOperatorPtr that is essentially just one logical stream
     */
    //OptimizerPlanOperator(SourceLogicalOperatorNodePtr source);

    OptimizerPlanOperator();

  private:
    /**
      * Cost issued by this operator
      * This is usually the output cardinality of, e.g., a join.
      * Sum of operators input sizes.
      */
    long operatorCosts;

    /**
      * Cost of the whole subplan cumulated.
      * OP cost of root + children's cumulative costs
      */
    long cumulativeCosts;

    /**
      * stream cardinality, i.e. size of tuples in window.
      */
    long cardinality;

    /**
     * not sure if needed
     */
    int id;

    static int current_id;

    /**
      * Operators below this operator.
      * i.e. if there was a join already covered, this would name the respective join partners.
      */
    std::set<OptimizerPlanOperator> involvedOptimizerPlanOperators;

    /**
     * Logical Source Operator
     */
    SourceLogicalOperatorNodePtr sourceNode;

  public:
    long getOperatorCosts() const;
    void setOperatorCosts(long operatorCosts);
    long getCumulativeCosts() const;
    void setCumulativeCosts(long cumulativeCosts);
    long getCardinality() const;
    void setCardinality(long cardinality);
    int getId() const;

    const std::set<OptimizerPlanOperator>& getInvolvedOptimizerPlanOperators() const;
    void setInvolvedOptimizerPlanOperators(const std::set<OptimizerPlanOperator>& involvedOptimizerPlanOperators);

    const SourceLogicalOperatorNodePtr& getSourceNode() const;
    void setSourceNode(const SourceLogicalOperatorNodePtr& sourceNode);

    bool operator<(const OptimizerPlanOperator& optimizerPlanOperator2) const {
        return this->getId() < optimizerPlanOperator2.getId();
    }
};

} // namespace NES
#endif//NES_OPTIMIZERPLANOPERATOR_HPP
