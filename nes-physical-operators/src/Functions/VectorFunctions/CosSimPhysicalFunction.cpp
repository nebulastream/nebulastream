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

#include <Functions/VectorFunctions/CosSimPhysicalFunction.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{
CosSimPhysicalFunction::CosSimPhysicalFunction(
    PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction, DataType outputType)
    : leftPhysicalFunction(std::move(leftPhysicalFunction))
    , rightPhysicalFunction(std::move(rightPhysicalFunction))
    , outputType(std::move(outputType))
{
}

namespace
{
/// Takes raw pointers and byte lengths for a pair of float32 vectors.
double computeCosineSimilarity(
    const int8_t* inputPtrLeft, const uint64_t inputSizeLeft, const int8_t* inputPtrRight, const uint64_t inputSizeRight)
{
    if (inputSizeLeft % sizeof(float) != 0 || inputSizeRight % sizeof(float) != 0)
    {
        throw FormattingError(
            "CosSim inputs must have byte sizes that are multiples of sizeof(float) ({}), but got left: {} bytes, right: {} bytes",
            sizeof(float),
            inputSizeLeft,
            inputSizeRight);
    }

    const auto* vectorLeft = reinterpret_cast<const float*>(inputPtrLeft);
    const auto* vectorRight = reinterpret_cast<const float*>(inputPtrRight);
    const uint64_t dimLeft = inputSizeLeft / sizeof(float);
    const uint64_t dimRight = inputSizeRight / sizeof(float);

    double dot = 0.0;
    double normLeft = 0.0;
    double normRight = 0.0;

    for (uint64_t i = 0; i < dimLeft && i < dimRight; ++i)
    {
        const auto vectorElementLeft = static_cast<double>(vectorLeft[i]);
        const auto vectorElementRight = static_cast<double>(vectorRight[i]);
        dot += vectorElementLeft * vectorElementRight;
        normLeft += vectorElementLeft * vectorElementLeft;
        normRight += vectorElementRight * vectorElementRight;
    }
    /// Following steps are a padding.
    /// Elements of the shorter vector are assumed to be 0.
    for (uint64_t i = dimLeft; i < dimRight; ++i)
    {
        const auto vectorElementRight = static_cast<double>(vectorRight[i]);
        normRight += vectorElementRight * vectorElementRight;
    }
    for (uint64_t i = dimRight; i < dimLeft; ++i)
    {
        const auto vectorElementLeft = static_cast<double>(vectorLeft[i]);
        normLeft += vectorElementLeft * vectorElementLeft;
    }

    /// This is more numerically stable than sqrt(norm1 * norm2).
    const double denom = std::sqrt(normLeft) * std::sqrt(normRight);
    if (denom <= 0.0)
    {
        return 0.0;
    }
    /// To even out numerical errors, the result is clamped.
    return std::clamp(dot / denom, -1.0, 1.0);
}
}

VarVal CosSimPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftVal = leftPhysicalFunction.execute(record, arena);
    const auto rightVal = rightPhysicalFunction.execute(record, arena);

    if (outputType.nullable)
    {
        const auto isNull = (leftVal.isNullable() && leftVal.isNull()) || (rightVal.isNullable() && rightVal.isNull());
        if (isNull)
        {
            return VarVal{0.0, true, true}.castToType(outputType.type);
        }
    }

    const auto leftValue = leftVal.getRawValueAs<VariableSizedData>();
    const auto rightValue = rightVal.getRawValueAs<VariableSizedData>();

    const auto similarity = nautilus::invoke(
        computeCosineSimilarity, leftValue.getContent(), leftValue.getSize(), rightValue.getContent(), rightValue.getSize());

    return VarVal{similarity, outputType.nullable, nautilus::val<bool>{false}}.castToType(outputType.type);
}

PhysicalFunctionRegistryReturnType CosSimPhysicalFunction::createCosSim(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "CosSim function must have exactly two child functions");
    return CosSimPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.childFunctions[1],
        physicalFunctionRegistryArguments.outputType);
}
}
