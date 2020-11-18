/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
#define NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_

#include <memory>
namespace NES::Windowing {
class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptorPtr;

}// namespace NES::Windowing

namespace NES {

class TranslateToGeneratableOperatorPhase;
typedef std::shared_ptr<TranslateToGeneratableOperatorPhase> TranslateToGeneratableOperatorPhasePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GeneratableOperator;
typedef std::shared_ptr<GeneratableOperator> GeneratableOperatorPtr;

class GeneratableWindowAggregation;
typedef std::shared_ptr<GeneratableWindowAggregation> GeneratableWindowAggregationPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

/**
 * @brief Translates a logical query plan to the generatable operator tree
 */
class TranslateToGeneratableOperatorPhase {
  public:
    /**
     * @brief Factory method to create a translation phase.
     */
    static TranslateToGeneratableOperatorPhasePtr create();
    /**
     * @brief Translates a operator node and all its children to the generatable representation.
     * @param operatorNode
     * @return Generatable Operator Tree
     */
    OperatorNodePtr transform(OperatorNodePtr operatorNode, OperatorNodePtr legacyParent = nullptr);

    /**
     * @brief Translates an individual operator to its generatable representation.
     * @param operatorNode
     * @return Generatable Operator
     */
    OperatorNodePtr transformIndividualOperator(OperatorNodePtr operatorNode, OperatorNodePtr generatableParentOperator);

    /**
    * @brief Translates an window operator to its generatable representation.
    * @param windowOperator
    * @return Generatable Operator
    */
    OperatorNodePtr transformWindowOperator(WindowOperatorNodePtr windowOperator, OperatorNodePtr generatableParentOperator);

    /**
    * @brief Translates an window aggregation to its generatable representation.
    * @param windowAggregationDescriptor
    * @return Generatable Operator
    */
    GeneratableWindowAggregationPtr
    transformWindowAggregation(Windowing::WindowAggregationDescriptorPtr windowAggregationDescriptor);

    TranslateToGeneratableOperatorPhase();
};

}// namespace NES

#endif//NES_INCLUDE_NODES_PHASES_TRANSLATETOGENERATABLEOPERATORS_HPP_
