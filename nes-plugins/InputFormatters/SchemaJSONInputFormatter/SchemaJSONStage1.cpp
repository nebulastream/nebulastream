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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include <simdjson.h>

#include <ErrorHandling.hpp>
#include <FieldOffsets.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{
namespace
{
/// Flat JSON objects never nest, so a tiny fixed depth is enough for simdjson's open-container stack.
constexpr size_t SCHEMA_JSON_MAX_DEPTH = 16;

[[nodiscard]] bool isJsonWhitespace(const char character) noexcept
{
    return character == ' ' || character == '\t' || character == '\n' || character == '\r';
}

/// Per-thread simdjson stage-1 state. The indexer object itself stays stateless (the query engine calls the
/// indexer concurrently), so the mutable structural-index buffer lives here in thread_local storage. It is
/// (re)allocated only when a larger region than seen before arrives, so the steady state does no allocation.
struct Stage1State
{
    std::unique_ptr<simdjson::internal::dom_parser_implementation> implementation;
    size_t capacity = 0;
};

thread_local Stage1State tlStage1;
}

void schemaJsonStage1IntoFieldOffsets(
    FieldOffsets<SCHEMA_JSON_NUM_OFFSETS_PER_FIELD>& fieldOffsets,
    const std::string_view region,
    const FieldIndex base,
    const uint32_t numFields)
{
    /// An empty region (e.g. two adjacent tuple delimiters at a buffer boundary) holds zero complete records;
    /// nothing to index, and simdjson would reject a zero capacity.
    if (region.empty())
    {
        return;
    }

    auto& state = tlStage1;

    /// (Re)allocate the structural-index buffer only when it must grow (it sizes the index array, unrelated to
    /// the input padding below).
    if (not state.implementation || state.capacity < region.size())
    {
        const auto allocError = simdjson::get_active_implementation()->create_dom_parser_implementation(
            region.size(), SCHEMA_JSON_MAX_DEPTH, state.implementation);
        if (allocError != simdjson::SUCCESS)
        {
            throw CannotFormatSourceData(
                "SchemaJSON: simdjson could not allocate stage-1 capacity for {} bytes: {}",
                region.size(),
                simdjson::error_message(allocError));
        }
        state.capacity = region.size();
    }

    /// Zero-copy: run stage 1 straight over the source bytes. simdjson's SIMD loads may read up to
    /// SIMDJSON_PADDING (64B) past `region.size()`, so those trailing bytes must be READABLE -- their content is
    /// irrelevant (stage 1 only emits structural indexes for positions < len). That padding is guaranteed by the
    /// source (raw engine buffers are over-allocated with SIMDJSON_PADDING trailing slack); `region` ends at the
    /// last tuple delimiter, at or before the buffer's data end, so [region.end(), region.end()+64) stays inside
    /// that slack. (This is the same contract the on-demand "JSON" indexer relies on; both drop the defensive
    /// per-buffer copy.)
    const auto* const bytes = reinterpret_cast<const uint8_t*>(region.data());
    const auto stage1Error = state.implementation->stage1(bytes, region.size(), simdjson::stage1_mode::regular);
    /// EMPTY (no structural element) is fine -- it just means zero complete records in this region.
    if (stage1Error != simdjson::SUCCESS && stage1Error != simdjson::EMPTY)
    {
        throw CannotFormatSourceData("SchemaJSON: simdjson stage-1 failed on JSON buffer: {}", simdjson::error_message(stage1Error));
    }

    const uint32_t* const structuralIndexes = state.implementation->structural_indexes.get();
    const uint32_t numStructuralIndexes = state.implementation->n_structural_indexes;

    /// A flat object of F fields emits exactly S = 4F + 1 structural indexes:
    ///   '{'  then F * ( key-quote, ':', value-start )  then (F-1) ',' then '}'  =  1 + 3F + (F-1) + 1.
    /// Under the fixed schema every record has this exact shape, so the total structural count is a multiple
    /// of S. Any shape deviation (missing/extra key, nested value, wrong arity) changes the count and is
    /// rejected here. This is a SHAPE check, not a value-validity check: a per-record reordering that keeps
    /// the count is not caught (SchemaJSON requires a consistent key order by contract).
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

    /// The record count is known now, so size the band in one shot -- the per-record emplaceTupleOffsets below
    /// then only grows the vector's size within this capacity, never reallocates (TWO offsets per field).
    fieldOffsets.reserveOffsets(static_cast<size_t>(numRecords) * numFields * 2);

    for (uint32_t record = 0; record < numRecords; ++record)
    {
        const uint32_t recordBase = record * structuralIndexesPerRecord;

        for (uint32_t field = 0; field < numFields; ++field)
        {
            /// Local layout within a record's S-index block: '{'=0, then for field f: key-quote=1+4f,
            /// ':'=2+4f, value-start=3+4f, terminator (',' or '}')=4+4f.
            const uint32_t colonSlot = recordBase + 2 + (4 * field);
            const uint32_t valueSlot = recordBase + 3 + (4 * field);

            INVARIANT(
                bytes[structuralIndexes[colonSlot]] == static_cast<uint8_t>(':'),
                "SchemaJSON: structural index misaligned -- expected ':' before the value of field {}",
                field);

            const uint32_t valueStart = structuralIndexes[valueSlot];
            /// The next structural entry is the ',' or '}' that terminates the value.
            uint32_t valueEnd = structuralIndexes[valueSlot + 1];
            /// Trim whitespace that sits between the value and the terminator (pretty-printed JSON); packed
            /// NDJSON hits this loop zero times.
            while (valueEnd > valueStart && isJsonWhitespace(static_cast<char>(bytes[valueEnd - 1])))
            {
                --valueEnd;
            }

            /// Append in place (no resize value-init); reserveOffsets() above guarantees no reallocation.
            fieldOffsets.emplaceFieldOffsetPair(static_cast<FieldIndex>(base + valueStart), static_cast<FieldIndex>(base + valueEnd));
        }
    }
}

}
