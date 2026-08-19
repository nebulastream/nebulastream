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

#include <UncompiledSchemaJSONFIF.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include <simdjson.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// Flat JSON objects never nest, so a tiny fixed depth is enough for simdjson's open-container stack
/// (mirrors the compiled SchemaJSONStage1).
constexpr size_t UNCOMPILED_SCHEMA_JSON_MAX_DEPTH = 16;
}

void UncompiledSchemaJSONFIF::indexRegion(
    const std::string_view region, const UncompiledFieldIndex regionBase, const bool regionHasTrailingPadding)
{
    /// An empty region (e.g. two adjacent tuple delimiters) holds zero complete records; nothing to index,
    /// and simdjson would reject a zero capacity. totalNumberOfTuples stays 0.
    if (region.empty())
    {
        return;
    }

    /// simdjson stage 1 may read up to SIMDJSON_PADDING (64B) past region.size(). Raw engine buffers come from
    /// 64B-aligned pool memory (safe over-read, the same zero-copy contract as the compiled SchemaJSON), but
    /// the sequential task's assembled spanning tuple is a bare std::string with no such slack -- copy those
    /// into an owned padded scratch first (once per leading record, ~one row).
    const char* bytes = region.data();
    if (not regionHasTrailingPadding)
    {
        if (paddedRegionCopy.size() < region.size() + simdjson::SIMDJSON_PADDING)
        {
            paddedRegionCopy.resize(region.size() + simdjson::SIMDJSON_PADDING);
        }
        std::memcpy(paddedRegionCopy.data(), region.data(), region.size());
        bytes = paddedRegionCopy.data();
    }

    /// (Re)allocate the structural-index buffer only when it must grow.
    if (not implementation || implementationCapacity < region.size())
    {
        const auto allocError = simdjson::get_active_implementation()->create_dom_parser_implementation(
            region.size(), UNCOMPILED_SCHEMA_JSON_MAX_DEPTH, implementation);
        if (allocError != simdjson::SUCCESS)
        {
            throw CannotFormatSourceData(
                "SequentialUncompiledSchemaJSON: simdjson could not allocate stage-1 capacity for {} bytes: {}",
                region.size(),
                simdjson::error_message(allocError));
        }
        implementationCapacity = region.size();
    }

    const auto stage1Error = implementation->stage1(reinterpret_cast<const uint8_t*>(bytes), region.size(), simdjson::stage1_mode::regular);
    /// EMPTY (no structural element) is fine -- it just means zero complete records in this region.
    if (stage1Error != simdjson::SUCCESS && stage1Error != simdjson::EMPTY)
    {
        throw CannotFormatSourceData(
            "SequentialUncompiledSchemaJSON: simdjson stage-1 failed on JSON buffer: {}", simdjson::error_message(stage1Error));
    }

    const uint32_t numStructuralIndexes = implementation->n_structural_indexes;

    /// Same SHAPE check as the compiled SchemaJSON: under the fixed schema the total structural-index count is
    /// a multiple of S = 4F+1; any shape deviation changes the count and is rejected here.
    if (structuralIndexesPerRecord == 0 || (numStructuralIndexes % structuralIndexesPerRecord) != 0)
    {
        throw CannotFormatSourceData(
            "SchemaJSON shape mismatch: {} structural indexes are not a multiple of {} (= 4*F+1 for F={} fields). "
            "SchemaJSON requires every record to be a flat JSON object with exactly the schema's {} keys, in the "
            "same order, with no missing/extra keys and no nested or null values. Use the 'JSON' formatter for "
            "irregular JSON.",
            numStructuralIndexes,
            structuralIndexesPerRecord,
            numFields,
            numFields);
    }

    this->structuralIndexes = implementation->structural_indexes.get();
    this->base = regionBase;
    this->totalNumberOfTuples = numStructuralIndexes / structuralIndexesPerRecord;
}

}
