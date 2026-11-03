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
#include <mutex>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
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

UdfStage::UdfStage(const std::vector<UdfDescriptor>& descriptors)
{
    PRECONDITION(!descriptors.empty(), "A UDF stage needs at least one UDF");
    std::size_t firstArgument = 0;
    for (const auto& descriptor : descriptors)
    {
        PRECONDITION(descriptor.getPath() == descriptors.front().getPath(), "The UDFs of a stage must share one bridge library");
        udfs.push_back(
            Udf{.backend = UdfBackend::create(descriptor),
                .firstArgument = firstArgument,
                .varsizedResult = descriptor.getReturnType().type == DataType::Type::VARSIZED});
        firstArgument += descriptor.getArgTypes().size();
    }
}

void UdfStage::executeRow(
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* results,
    std::int8_t* resultLens,
    std::int8_t* resultNulls)
{
    const std::scoped_lock lock(mutex);
    const RowScope scope(*udfs.front().backend);
    for (std::size_t i = 0; i < udfs.size(); ++i)
    {
        const auto& udf = udfs[i];
        const auto* const values = argValues + (udf.firstArgument * SLOT_BYTES);
        const auto* const lens = argLens + (udf.firstArgument * SLOT_BYTES);
        const auto* const nulls = argNulls + udf.firstArgument;
        auto* const result = results + (i * SLOT_BYTES);
        int resultNull = 0;
        if (udf.varsizedResult)
        {
            std::int8_t* buffer = nullptr;
            std::memcpy(static_cast<void*>(&buffer), result, sizeof(buffer));
            const std::uint64_t length
                = udf.backend->executeVarsizedRow(values, lens, nulls, buffer, MAX_VARSIZED_RESULT_BYTES, &resultNull);
            std::memcpy(resultLens + (i * SLOT_BYTES), &length, sizeof(length));
        }
        else
        {
            udf.backend->executeScalarRow(values, lens, nulls, result, &resultNull);
        }
        resultNulls[i] = static_cast<std::int8_t>(resultNull);
    }
}

}

/// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
