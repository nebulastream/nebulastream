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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{
/// Assembles a dense float32 tensor of a fixed shape from the tuples of a window, e.g. as the input of a model.
/// SQL: TENSOR_AGG(v_1, ..., v_k AT (i_1, ..., i_m) SHAPE (d_1, ..., d_m, k) DEFAULT x)
/// Every tuple writes its value fields v_1..v_k along the last axis, at the position its index fields i_1..i_m select
/// along the leading axes. With a single value field, the trailing axis of length 1 may be left out of SHAPE. Positions come from the tuples, not from their arrival order, so the result does not depend
/// on sources, threads or buffer boundaries. Cells that no tuple writes keep the default value, so the result always
/// holds exactly d_1 * ... * d_m * k float32 values in row-major order.
/// Tuples with an index outside the shape or a NULL index are ignored; a NULL value leaves its cell untouched.
/// If several tuples write the same position, one of them wins.
class TensorAggAggregationLogicalFunction
{
public:
    TensorAggAggregationLogicalFunction(
        std::vector<AggregationFieldAccess> valueFunctions,
        std::vector<AggregationFieldAccess> indexFunctions,
        std::vector<uint64_t> shape,
        float defaultValue);

    [[nodiscard]] TensorAggAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::vector<AggregationFieldAccess> getInputFunctions() const;
    [[nodiscard]] const std::vector<AggregationFieldAccess>& getValueFunctions() const;
    [[nodiscard]] const std::vector<AggregationFieldAccess>& getIndexFunctions() const;
    [[nodiscard]] const std::vector<uint64_t>& getShape() const;
    [[nodiscard]] float getDefaultValue() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const TensorAggAggregationLogicalFunction& other) const;
    static constexpr uint64_t MAX_NUMBER_OF_VALUES = uint64_t{1} << 28U;

private:
    std::vector<AggregationFieldAccess> valueFunctions;
    std::vector<AggregationFieldAccess> indexFunctions;
    std::vector<uint64_t> shape;
    float defaultValue;
    static constexpr std::string_view NAME = "TensorAgg";
};

template <>
struct Reflector<TensorAggAggregationLogicalFunction>
{
    Reflected operator()(const TensorAggAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TensorAggAggregationLogicalFunction>
{
    TensorAggAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::TensorAggAggregationLogicalFunction>
{
    size_t operator()(const NES::TensorAggAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::TensorAggAggregationLogicalFunction>);
