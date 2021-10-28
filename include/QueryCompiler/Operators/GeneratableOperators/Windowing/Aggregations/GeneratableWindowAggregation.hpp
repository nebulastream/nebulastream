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

#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_WINDOW_AGGREGATION_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_WINDOW_AGGREGATION_HPP_
#include <QueryCompiler/CodeGenerator/RecordHandler.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <memory>

namespace NES::Windowing {
class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

}// namespace NES::Windowing

namespace NES {

class BinaryOperatorStatement;
class StructDeclaration;
class CompoundStatement;
using CompoundStatementPtr = std::shared_ptr<CompoundStatement>;
namespace QueryCompilation {
namespace GeneratableOperators {
class GeneratableWindowAggregation {
  public:
    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    virtual void compileLiftCombine(CompoundStatementPtr currentCode,
                                    BinaryOperatorStatement partialValueRef,
                                    RecordHandlerPtr recordHandler) = 0;

    virtual ~GeneratableWindowAggregation() noexcept = default;

  protected:
    explicit GeneratableWindowAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);
    Windowing::WindowAggregationDescriptorPtr aggregationDescriptor;
};

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_WINDOW_AGGREGATION_HPP_
