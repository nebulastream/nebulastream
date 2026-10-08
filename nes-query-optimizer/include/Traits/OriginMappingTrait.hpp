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
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// What one branch of a fan-out point stamps its buffers with, as pairs of the origin id the branch reads and the id
/// that replaces it. Carried by the OriginSplit operator heading the branch, and turned into the stamping step during
/// lowering.
///
/// The mapping is one-to-one rather than collapsing the read origins onto a single id, because an emit keeps the
/// sequence number of the buffer it read and sequence numbers are unique only within an origin.
struct OriginMappingTrait final
{
    static constexpr std::string_view NAME = "OriginMapping";
    std::vector<std::pair<OriginId, OriginId>> originMapping;

    explicit OriginMappingTrait(std::vector<std::pair<OriginId, OriginId>> originMapping);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const OriginMappingTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<OriginMappingTrait>;
};

template <>
struct Reflector<OriginMappingTrait>
{
    Reflected operator()(const OriginMappingTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<OriginMappingTrait>
{
    OriginMappingTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<OriginMappingTrait>);

}

namespace NES::detail
{
struct ReflectedOriginMappingTrait
{
    /// The mapping as two parallel vectors, so that the reflected form holds plain integers.
    std::vector<uint64_t> upstreamOriginIds;
    std::vector<uint64_t> branchOriginIds;
};
}
