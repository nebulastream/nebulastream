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


#ifndef NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ABSTRACTFILTEROPERATOR_HPP_
#define NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ABSTRACTFILTEROPERATOR_HPP_

#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES{

/**
 * @brief Abstract filter operator with a filter predicate.
 * A filter operator evaluates the predicate for each input record and selects all records
 * where the predicate qualifies.
 */
class AbstractFilterOperator {

  public:
    /**
     * @brief get the filter predicate.
     * @return PredicatePtr
     */
    ExpressionNodePtr getPredicate();

  protected:
    AbstractFilterOperator(ExpressionNodePtr predicate);
    ExpressionNodePtr predicate;

};

}

#endif//NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ABSTRACTFILTEROPERATOR_HPP_
