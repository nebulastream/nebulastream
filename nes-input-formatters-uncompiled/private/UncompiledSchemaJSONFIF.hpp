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
#include <limits>
#include <memory>
#include <string_view>
#include <vector>

#include <simdjson.h>
#include <UncompiledFieldIndexFunction.hpp>

namespace NES
{

/// Uncompiled (plain-C++) analog of the compiled SchemaJSONFieldIndex: the SAME simdjson stage-1 structural
/// indexing and the SAME fixed-stride addressing (a flat object of F fields emits S = 4F+1 structural indexes;
/// value f is the entry at local slot 3+4f, terminated by the next entry), read back with an interpreted
/// per-field loop instead of the compiled strided read. This makes the uncompiled rung differ from the
/// compiled SchemaJSON ONLY in interpretation vs compilation -- not in indexing algorithm (the same split as
/// UncompiledFieldBand vs the compiled FieldBand for CSV). Unlike UncompiledSIMDJSONFIF (simdjson On-Demand,
/// navigates by key name), this FIF never looks at key names.
///
/// Ownership: the FIF is constructed fresh per raw buffer (stack instance in the task), so it OWNS its
/// dom_parser_implementation -- no thread_local slot pool as in the compiled plugin (the compiled pool exists
/// because three FIF roles live in one static thread_local IndexPhaseResult; here each instance is private).
class UncompiledSchemaJSONFIF final : public UncompiledFieldIndexFunction<UncompiledSchemaJSONFIF>
{
    friend UncompiledFieldIndexFunction<UncompiledSchemaJSONFIF>;

    /// --- UncompiledFieldIndexFunction (CRTP) read interface ---

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] UncompiledFieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    [[nodiscard]] size_t applyGetTotalNumberOfTuples() const { return this->totalNumberOfTuples; }

    [[nodiscard]] bool applyHasNext(const size_t tupleIdx) const { return tupleIdx < this->totalNumberOfTuples; }

    [[nodiscard]] std::string_view applyReadFieldAt(const std::string_view bufferView, const size_t tupleIdx, const size_t fieldIdx) const
    {
        const size_t recordBase = tupleIdx * structuralIndexesPerRecord;
        const auto valueStart = structuralIndexes[recordBase + 3 + (4 * fieldIdx)];
        auto valueEnd = structuralIndexes[recordBase + 4 + (4 * fieldIdx)];
        /// Right-trim JSON whitespace between the value and its terminator (pretty-printed JSON; packed NDJSON
        /// does 0 decrements) -- mirrors the compiled read-time trim, required for bools and quoted strings.
        while (valueEnd > valueStart)
        {
            const char prev = bufferView[static_cast<size_t>(base) + valueEnd - 1];
            if (prev == ' ' || prev == '\t' || prev == '\n' || prev == '\r')
            {
                --valueEnd;
            }
            else
            {
                break;
            }
        }
        return bufferView.substr(static_cast<size_t>(base) + valueStart, valueEnd - valueStart);
    }

public:
    UncompiledSchemaJSONFIF() = default;
    ~UncompiledSchemaJSONFIF() = default;

    /// --- write-side contract (called by SequentialUncompiledSchemaJSONInputFormatIndexer) ---

    void startSetup(const uint32_t numberOfFieldsInSchema)
    {
        this->numFields = numberOfFieldsInSchema;
        this->structuralIndexesPerRecord = (4 * numberOfFieldsInSchema) + 1;
        this->totalNumberOfTuples = 0;
        this->structuralIndexes = nullptr;
        this->base = 0;
    }

    void markNoTupleDelimiters()
    {
        this->offsetOfFirstTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->offsetOfLastTuple = std::numeric_limits<UncompiledFieldIndex>::max();
        this->totalNumberOfTuples = 0;
    }

    void markWithTupleDelimiters(const UncompiledFieldIndex offsetToFirstTuple, const UncompiledFieldIndex offsetToLastTuple)
    {
        this->offsetOfFirstTuple = offsetToFirstTuple;
        this->offsetOfLastTuple = offsetToLastTuple;
    }

    /// Runs simdjson stage 1 over `region` (the bytes of all COMPLETE records, i.e. between the first and last
    /// tuple delimiter of the buffer view) and records the structural-index base + record count. `regionBase`
    /// is the absolute offset of `region` within the buffer view (added back at read time). When
    /// `regionHasTrailingPadding` is false (the sequential task's assembled spanning-tuple std::string, which
    /// has no over-read slack), the region is first copied into an owned SIMDJSON_PADDING-padded scratch.
    void indexRegion(std::string_view region, UncompiledFieldIndex regionBase, bool regionHasTrailingPadding);

private:
    std::unique_ptr<simdjson::internal::dom_parser_implementation> implementation;
    size_t implementationCapacity{0};
    std::vector<char> paddedRegionCopy;
    const uint32_t* structuralIndexes{nullptr}; ///< non-owning; into `implementation` (valid until the next indexRegion)
    UncompiledFieldIndex base{}; ///< absolute offset of the indexed region within the buffer view
    uint32_t numFields{};
    uint32_t structuralIndexesPerRecord{}; ///< 4F+1
    size_t totalNumberOfTuples{};
    UncompiledFieldIndex offsetOfFirstTuple{};
    UncompiledFieldIndex offsetOfLastTuple{};
};

}
