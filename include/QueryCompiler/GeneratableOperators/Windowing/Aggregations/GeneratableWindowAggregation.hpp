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

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
#include <memory>

namespace NES::Windowing {
class WindowAggregationDescriptor;
typedef std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptorPtr;

}// namespace NES::Windowing

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
typedef std::shared_ptr<CompoundStatement> CompoundStatementPtr;

class GeneratableWindowAggregation {
  public:
    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    virtual void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement partialValueRef,
                                    StructDeclaration inputStruct, BinaryOperatorStatement inputRef) = 0;

  protected:
    explicit GeneratableWindowAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);

    Windowing::WindowAggregationDescriptorPtr aggregationDescriptor;
};

typedef std::shared_ptr<GeneratableWindowAggregation> GeneratableWindowAggregationPtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEWINDOWAGGREGATION_HPP_
