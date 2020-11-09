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

#ifndef NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

}// namespace NES

namespace NES::Windowing {

/**
 * @brief A executable window aggregation, which is typed for the correct input, partial, and final data types.
 * @tparam InputType input type of the aggregation
 * @tparam PartialAggregateType partial aggregation type
 * @tparam FinalAggregateType final aggregation type
 */
template<class InputType, class PartialAggregateType, class FinalAggregateType>
class ExecutableWindowAggregation {

  public:
    /*
     * @brief maps the input element to an element PartialAggregateType
     * @param input value of the element
     * @return the element that mapped to PartialAggregateType
     */
    virtual PartialAggregateType lift(InputType input) = 0;

    /*
    * @brief combines two partial aggregates to a new partial aggregate
    * @param current partial value
    * @param the new input element
    * @return new partial aggregate as combination of partialValue and inputValue
    */
    virtual PartialAggregateType combine(PartialAggregateType partialAggregate1, PartialAggregateType partialAggregate2) = 0;

    /*
    * @brief maps partial aggregates to an element of FinalAggregationType
    * @param partial aggregate element
    * @return element mapped to FinalAggregationType
    */
    virtual FinalAggregateType lower(PartialAggregateType partialAggregate) = 0;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_EXECUTABLEWINDOWAGGREGATION_HPP_
