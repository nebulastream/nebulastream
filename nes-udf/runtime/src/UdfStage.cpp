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

#include <UdfStage.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ranges>
#include <span>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <InProcessBackend.hpp>
#include <UdfAbi.h>
#include <UdfBackend.hpp>
#include <UdfDescriptor.hpp>

/// Reads and writes the flat buffers described in UdfStage.hpp.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)

namespace NES
{

namespace
{
/// Leaves the bridge's row scope even if a UDF throws.
class RowScope
{
public:
    explicit RowScope(UdfBackend& backend) : backend(backend), state(backend.enterRow()) { }

    ~RowScope() { backend.exitRow(state); }

    RowScope(const RowScope&) = delete;
    RowScope& operator=(const RowScope&) = delete;
    RowScope(RowScope&&) = delete;
    RowScope& operator=(RowScope&&) = delete;

private:
    UdfBackend& backend;
    int state;
};
}

UdfStage::UdfStage(const std::vector<UdfDescriptor>& descriptors, const UdfInterpreter interpreter, const bool fuse)
{
    PRECONDITION(!descriptors.empty(), "A UDF stage needs at least one UDF");
    std::size_t firstArgument = 0;
    for (const auto& descriptor : descriptors)
    {
        PRECONDITION(descriptor.getPath() == descriptors.front().getPath(), "The UDFs of a stage must share one bridge library");
        udfs.push_back(
            Udf{.backend = UdfBackend::create(descriptor, interpreter),
                .firstArgument = firstArgument,
                .varsizedResult = descriptor.getReturnType().type == DataType::Type::VARSIZED});
        firstArgument += descriptor.getArgTypes().size();
    }
    if (fuse)
    {
        fused = std::make_unique<InProcessFusedUdfs>(
            udfs
            | std::views::transform(
                [](const Udf& udf)
                {
                    auto backend = std::dynamic_pointer_cast<InProcessBackend>(udf.backend);
                    INVARIANT(backend != nullptr, "Only in-process UDFs can be fused");
                    return backend;
                })
            | std::ranges::to<std::vector>());
    }
}

UdfStage::~UdfStage() = default;

void UdfStage::executeRow(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* results,
    std::int8_t* resultLens,
    std::int8_t* resultNulls) const
{
    /// Per thread, so that the result bytes need no allocation per row; the caller copies them out before the next row.
    thread_local std::vector<std::vector<std::int8_t>> varsizedResults;
    if (varsizedResults.size() < udfs.size())
    {
        varsizedResults.resize(udfs.size());
        /// So that even an empty result has a valid pointer for the caller to copy from.
        for (auto& result : varsizedResults)
        {
            result.reserve(64);
        }
    }
    const std::span stageResults(varsizedResults.data(), udfs.size());

    const RowScope scope(*udfs.front().backend);
    if (fused != nullptr)
    {
        fused->executeRow(argValues, argLens, argNulls, results, resultNulls, stageResults);
    }
    else
    {
        for (std::size_t i = 0; i < udfs.size(); ++i)
        {
            const auto& udf = udfs[i];
            const auto* const values = argValues + (udf.firstArgument * SLOT_BYTES);
            const auto* const lens = argLens + (udf.firstArgument * SLOT_BYTES);
            const auto* const nulls = argNulls + udf.firstArgument;
            int resultNull = 0;
            if (udf.varsizedResult)
            {
                udf.backend->executeVarsizedRow(values, lens, nulls, stageResults[i], &resultNull);
            }
            else
            {
                udf.backend->executeScalarRow(values, lens, nulls, results + (i * SLOT_BYTES), &resultNull);
            }
            resultNulls[i] = static_cast<std::int8_t>(resultNull);
        }
    }

    for (std::size_t i = 0; i < udfs.size(); ++i)
    {
        if (udfs[i].varsizedResult)
        {
            const std::int8_t* const content = stageResults[i].data();
            const std::uint64_t length = stageResults[i].size();
            std::memcpy(results + (i * SLOT_BYTES), static_cast<const void*>(&content), sizeof(content));
            std::memcpy(resultLens + (i * SLOT_BYTES), &length, sizeof(length));
        }
    }
}

void UdfStage::executeBatch(const std::size_t udf, ArrowArray* arguments, ArrowArray* result) const
{
    auto& backend = *udfs.at(udf).backend;
    const RowScope scope(backend);
    backend.executeBatch(arguments, result);
}

void UdfStage::executeFusedBatch(ArrowArray* arguments, ArrowArray* result) const
{
    PRECONDITION(fused != nullptr, "Only a fused UDF stage runs all UDFs on a batch at once");
    const RowScope scope(*udfs.front().backend);
    fused->executeBatch(arguments, result);
}

}

/// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
