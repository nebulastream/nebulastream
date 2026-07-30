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

#include <SchemaJSONInputFormatIndexer.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include <simdjson.h>

#include <ErrorHandling.hpp>
#include <SchemaJSONFieldIndex.hpp>

namespace NES
{
namespace
{
/// Flat JSON objects never nest, so a tiny fixed depth is enough for simdjson's open-container stack.
constexpr size_t SCHEMA_JSON_MAX_DEPTH = 16;

/// One persistent simdjson stage-1 buffer per FIF ROLE per thread. In "option 2" the structural indexes must
/// survive from the indexing phase into the read phase (the read gathers (start,end) from them on the fly),
/// and a single readBuffer pass reads THREE distinct index sets (rawBufferFIF + leading + trailing spanning
/// FIFs) -- so they must not share one buffer. Keying by the FIF's address gives each role its own persistent
/// buffer: the three FIFs are fixed members of the InputFormatter's static thread_local IndexPhaseResult, so
/// their addresses are stable across buffers even though the POD FIF state is reset each buffer. The buffer is
/// (re)allocated only when a larger region than seen before arrives, so the steady state does no allocation
/// (mirrors the baseline's persistent tlStage1).
struct Stage1Slot
{
    const void* owner = nullptr;
    std::unique_ptr<simdjson::internal::dom_parser_implementation> implementation;
    size_t capacity = 0;
};

/// At most three distinct owners exist per (thread, SchemaJSON formatter); 8 is comfortable headroom.
thread_local std::array<Stage1Slot, 8> tlStage1Pool;

Stage1Slot& acquireSlotFor(const void* const owner)
{
    for (auto& slot : tlStage1Pool)
    {
        if (slot.owner == owner)
        {
            return slot;
        }
    }
    for (auto& slot : tlStage1Pool)
    {
        if (slot.owner == nullptr)
        {
            slot.owner = owner;
            return slot;
        }
    }
    throw CannotFormatSourceData("SchemaJSON: stage-1 slot pool exhausted (more than {} concurrent FIF roles)", tlStage1Pool.size());
}
}

void schemaJsonIndexIntoFieldIndex(
    SchemaJSONFieldIndex& fieldIndex, const std::string_view region, const FieldIndex base, const uint32_t numFields)
{
    /// An empty region (e.g. two adjacent tuple delimiters at a buffer boundary) holds zero complete records;
    /// nothing to index, and simdjson would reject a zero capacity. The FIF keeps totalNumberOfTuples == 0.
    if (region.empty())
    {
        return;
    }

    auto& slot = acquireSlotFor(&fieldIndex);

    /// (Re)allocate the structural-index buffer only when it must grow.
    if (not slot.implementation || slot.capacity < region.size())
    {
        const auto allocError = simdjson::get_active_implementation()->create_dom_parser_implementation(
            region.size(), SCHEMA_JSON_MAX_DEPTH, slot.implementation);
        if (allocError != simdjson::SUCCESS)
        {
            throw CannotFormatSourceData(
                "SchemaJSON: simdjson could not allocate stage-1 capacity for {} bytes: {}",
                region.size(),
                simdjson::error_message(allocError));
        }
        slot.capacity = region.size();
    }

    /// Zero-copy: run stage 1 straight over the source bytes. simdjson's SIMD loads may read up to
    /// SIMDJSON_PADDING (64B) past `region.size()`; that padding is guaranteed by the source (raw engine
    /// buffers are over-allocated with SIMDJSON_PADDING trailing slack), same contract as before.
    const auto* const bytes = reinterpret_cast<const uint8_t*>(region.data());
    const auto stage1Error = slot.implementation->stage1(bytes, region.size(), simdjson::stage1_mode::regular);
    /// EMPTY (no structural element) is fine -- it just means zero complete records in this region.
    if (stage1Error != simdjson::SUCCESS && stage1Error != simdjson::EMPTY)
    {
        throw CannotFormatSourceData("SchemaJSON: simdjson stage-1 failed on JSON buffer: {}", simdjson::error_message(stage1Error));
    }

    const uint32_t* const structuralIndexes = slot.implementation->structural_indexes.get();
    const uint32_t numStructuralIndexes = slot.implementation->n_structural_indexes;

    /// A flat object of F fields emits exactly S = 4F + 1 structural indexes:
    ///   '{'  then F * ( key-quote, ':', value-start )  then (F-1) ',' then '}'  =  1 + 3F + (F-1) + 1.
    /// Under the fixed schema the total is a multiple of S; any shape deviation changes the count and is
    /// rejected here (a SHAPE check, not a value-validity check).
    const uint32_t structuralIndexesPerRecord = (4 * numFields) + 1;
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
    const uint32_t numRecords = numStructuralIndexes / structuralIndexesPerRecord;

    /// No gather: hand the read path a NON-owning pointer into simdjson's own structural_indexes array; the
    /// read computes (start,end) on the fly via the fixed 4F+1 stride, touching only projected fields.
    fieldIndex.setStage1Result(structuralIndexes, base, numRecords);
}

}
