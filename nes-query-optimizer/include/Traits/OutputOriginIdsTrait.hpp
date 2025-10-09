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
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableTrait.pb.h>

namespace NES
{

class OutputOriginIdsTrait final : public TraitConcept
{
public:
    static constexpr std::string_view NAME = "OutputOriginIds";
    explicit OutputOriginIdsTrait(std::vector<OriginId> originIds);

    [[nodiscard]] const std::type_info& getType() const override;
    [[nodiscard]] std::string_view getName() const override;
    [[nodiscard]] SerializableTrait serialize() const override;
    bool operator==(const TraitConcept& other) const override;
    [[nodiscard]] size_t hash() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<OriginId>>().cbegin());
    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<OriginId>>().cend());

    OriginId& operator[](size_t index);
    const OriginId& operator[](size_t index) const;
    [[nodiscard]] size_t size() const;

private:
    std::vector<OriginId> originIds;
};

}
