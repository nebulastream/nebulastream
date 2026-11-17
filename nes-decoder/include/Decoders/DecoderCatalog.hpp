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

#include <expected>
#include <mutex>
#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Decoders/DecoderStageDescriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// A named decoder (CREATE DECODER name AS (stage, ...) SET(...)): its stages in chain order, each with its
/// instantiated config.
struct NamedDecoder
{
    Identifier name;
    std::vector<DecoderStageDescriptor> stages;

    bool operator==(const NamedDecoder&) const = default;
    friend std::ostream& operator<<(std::ostream& out, const NamedDecoder& decoder);
};

/// The named decoders of a frontend. A physical source that references a decoder (SOURCE.DECODER) gets a copy of
/// its stages when it is created, so dropping or recreating a decoder never affects existing sources.
class DecoderCatalog
{
public:
    /// Validates the stages and resolves the options; nothing is registered if either fails.
    [[nodiscard]] std::expected<NamedDecoder, Exception>
    addDecoder(const Identifier& name, const std::vector<Identifier>& stageTypes, const Schema<LiteralConfigValue, Ordered>& options);

    [[nodiscard]] std::optional<NamedDecoder> getDecoder(const Identifier& name) const;
    [[nodiscard]] std::vector<NamedDecoder> getAllDecoders() const;
    /// @returns false if no decoder of that name exists
    bool removeDecoder(const Identifier& name);

private:
    mutable std::mutex mutex;
    std::unordered_map<Identifier, NamedDecoder> decoders;
};

}

FMT_OSTREAM(NES::NamedDecoder);
