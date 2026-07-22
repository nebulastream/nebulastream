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

#include <Functions/ConcatPhysicalFunction.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/RopeLazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

ConcatPhysicalFunction::ConcatPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal ConcatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);

    /// Whether either operand can be null is a host/trace-time property (VarVal::nullable is a plain bool), so
    /// this drives compile-time control flow -- no runtime branch, no variant-merge across the two returns.
    const bool anyNullable = leftValue.isNullable() or rightValue.isNullable();

    /// Instead of eagerly allocating an arena buffer and copying both operands into it (input -> arena, then
    /// arena -> output in the formatter = the payload copied twice), build a rope: the ordered list of the
    /// operands' byte spans, materialised only if some non-serialize consumer needs contiguous bytes. The
    /// formatter walks the spans and writes each straight to the output -> the payload is copied ONCE.
    /// Collecting spans never materialises AND emits no traced op (it only copies SSA handles into a host
    /// vector): a lazy operand contributes its span list (a nested rope FLATTENS here -- CONCAT(CONCAT(a,b),c)
    /// -> {a,b,c} with no intermediate copy; a passthrough contributes one view-span), a non-lazy operand (a
    /// materialised VariableSizedData or a VARSIZED literal) is one span.
    std::vector<LazyValueRepresentation::Span> spans;
    const auto appendOperand = [&spans](const VarVal& operand)
    {
        if (operand.isLazyValue())
        {
            const auto lazyOperand = operand.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
            const auto operandSpans = lazyOperand->getSpans();
            spans.insert(spans.end(), operandSpans.begin(), operandSpans.end());
        }
        else
        {
            const auto variableSizedData = operand.getRawValueAs<VariableSizedData>();
            spans.push_back(LazyValueRepresentation::Span{variableSizedData.getContent(), variableSizedData.getSize()});
        }
    };
    appendOperand(leftValue);
    appendOperand(rightValue);

    /// Σ segment lengths, computed from each operand's total size rather than a per-span accumulation loop:
    /// a per-span traced `+=` at one source line, run once per span, would look like a loop back-edge to the
    /// nautilus tracer ("constant loop"). getSize() reads the operand's cached length (a lazy value's member,
    /// a rope's Σ, or a VariableSizedData's size) with no walk. This is a single traced add, emitted once.
    const auto operandTotalSize = [](const VarVal& operand) -> nautilus::val<uint64_t>
    {
        if (operand.isLazyValue())
        {
            return operand.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>()->getSize();
        }
        return operand.getRawValueAs<VariableSizedData>().getSize();
    };
    const auto totalSize = operandTotalSize(leftValue) + operandTotalSize(rightValue);

    const DataType resultType{DataType::Type::VARSIZED, anyNullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE};

    if (anyNullable)
    {
        const auto newNull = leftValue.isNull() or rightValue.isNull();
        /// Preserve the eager path's null-size semantics (a null CONCAT has logical size 0). The formatter
        /// short-circuits on isNull before ever walking the spans, so the segment contents on the null path
        /// are never read.
        const auto ropeSize = nautilus::select(newNull, nautilus::val<uint64_t>{0}, totalSize);
        auto rope = std::make_shared<RopeLazyValueRepresentation>(std::move(spans), ropeSize, arena.getArena(), resultType, newNull);
        return VarVal{std::static_pointer_cast<LazyValueRepresentation>(rope), true, newNull};
    }
    auto rope = std::make_shared<RopeLazyValueRepresentation>(
        std::move(spans), totalSize, arena.getArena(), resultType, nautilus::val<bool>{false});
    return VarVal{std::static_pointer_cast<LazyValueRepresentation>(rope), false, nautilus::val<bool>{false}};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConcatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Concat function must have exactly two child functions");
    return ConcatPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}
}
