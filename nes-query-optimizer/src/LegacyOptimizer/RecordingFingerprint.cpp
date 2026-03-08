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

#include <LegacyOptimizer/RecordingFingerprint.hpp>

#include <cstdint>
#include <string>

#include <Plans/LogicalPlan.hpp>
#include <QueryId.hpp>
#include <fmt/format.h>

namespace NES
{

namespace
{
constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

uint64_t fnv1a64(std::string_view input)
{
    uint64_t hash = FNV_OFFSET_BASIS;
    for (const auto character : input)
    {
        hash ^= static_cast<uint8_t>(character);
        hash *= FNV_PRIME;
    }
    return hash;
}

std::string encodeRetentionCoverage(const std::optional<ReplaySpecification>& replaySpecification)
{
    return fmt::format("retention_ms={}", replaySpecification.and_then([](const auto& spec) { return spec.retentionWindowMs; }).value_or(0));
}

std::string encodeRepresentation(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "binary_store";
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return "binary_store_zstd_fast1";
        case RecordingRepresentation::BinaryStoreZstd:
            return "binary_store_zstd";
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return "binary_store_zstd_fast6";
    }
    std::unreachable();
}
}

std::string createStructuralRecordingFingerprint(const LogicalOperator& recordedSubplanRoot, const Host& placement)
{
    const auto recordedPlan = explain(LogicalPlan(INVALID_QUERY_ID, {recordedSubplanRoot}), ExplainVerbosity::Short);
    const auto canonical = fmt::format(
        "placement={}|schema={}|plan={}",
        placement,
        recordedSubplanRoot.getOutputSchema(),
        recordedPlan);
    return fmt::format("{:016x}", fnv1a64(canonical));
}

std::string createRecordingFingerprint(
    const LogicalOperator& recordedSubplanRoot,
    const Host& placement,
    const std::optional<ReplaySpecification>& replaySpecification,
    const RecordingRepresentation representation)
{
    const auto canonical = fmt::format(
        "structural={}|representation={}|coverage={}",
        createStructuralRecordingFingerprint(recordedSubplanRoot, placement),
        encodeRepresentation(representation),
        encodeRetentionCoverage(replaySpecification));
    return fmt::format("{:016x}", fnv1a64(canonical));
}

RecordingId recordingIdFromFingerprint(std::string_view fingerprint)
{
    return RecordingId{fmt::format("recording-{}", fingerprint)};
}

}
