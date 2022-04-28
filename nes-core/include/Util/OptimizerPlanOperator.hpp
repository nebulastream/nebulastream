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

class OptimizerPlanOperator;
using OptimizerPlanOperatorPtr = std::shared_ptr<OptimizerPlanOperator>;

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
     * increasing id
     */
    int id;

  private:
    static int current_id;

    /**
     * Logical Source Operator
     */
    SourceLogicalOperatorNodePtr sourceNode;

  public:
    int getId() const;
    void setId(int id);

    const SourceLogicalOperatorNodePtr& getSourceNode() const;
    void setSourceNode(const SourceLogicalOperatorNodePtr& sourceNode);

    bool operator<(const OptimizerPlanOperator& optimizerPlanOperator2) const {
        return this->getId() < optimizerPlanOperator2.getId();
    }
};

} // namespace NES
#endif//NES_OPTIMIZERPLANOPERATOR_HPP
