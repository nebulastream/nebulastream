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
#include <vector>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// The fields of the Arrow batches of a UDF process that stay in the row region of a hybrid buffer, see ArrowBufferRef: their
/// positions in the field ordering of the operator. The sink and the source of a UDF process carry it, so that the emit before the
/// process and the scan after it lower the batches alike.
struct RowFieldsTrait final
{
    static constexpr std::string_view NAME = "RowFields";
    std::vector<uint64_t> positions;

    explicit RowFieldsTrait(std::vector<uint64_t> positions);

    [[nodiscard]] const std::type_info& getType() const;

    bool operator==(const RowFieldsTrait& other) const;

    [[nodiscard]] size_t hash() const;

    [[nodiscard]] std::string explain(ExplainVerbosity) const;

    [[nodiscard]] std::string_view getName() const;

    friend Reflector<RowFieldsTrait>;
};

template <>
struct Reflector<RowFieldsTrait>
{
    Reflected operator()(const RowFieldsTrait& trait, const ReflectionContext& context) const;
};

template <>
struct Unreflector<RowFieldsTrait>
{
    RowFieldsTrait operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(TraitConcept<RowFieldsTrait>);

}

namespace NES::detail
{
struct ReflectedRowFieldsTrait
{
    std::vector<uint64_t> positions;
};
}
