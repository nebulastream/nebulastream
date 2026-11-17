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
#include <memory>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Decoders/Decoder.hpp>
#include <Decoders/DecoderStageDescriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <ErrorHandling.hpp>

/// The decoder chain of a named decoder (CREATE DECODER name AS (LZ4, IntelliVue) SET(...)): an ordered list of
/// decoder stage names plus the stages' options. These helpers turn it into the stage descriptors (when the decoder
/// is created) and finally the running Decoder (when a source is lowered).
namespace NES::DecoderChain
{

/// Every name must be a registered stage, and no name may appear twice (the config fields of a stage are named
/// after its type, so two instances would share them). At least one stage is required.
[[nodiscard]] std::expected<void, Exception> validate(const std::vector<Identifier>& stageTypes);

/// The config fields declared by the given stages, in chain order.
[[nodiscard]] std::expected<Schema<QualifiedErasedConfigField, Ordered>, Exception> configSchema(const std::vector<Identifier>& stageTypes);

/// The descriptor of every stage, with its config struct instantiated from the resolved config.
[[nodiscard]] std::expected<std::vector<DecoderStageDescriptor>, Exception>
instantiate(const std::vector<Identifier>& stageTypes, const InstantiatedConfig& config);

/// validate + resolve the options (qualified by stage name, e.g. ZSTD.WINDOW_LOG_MAX) against the stages' config
/// fields + instantiate. Options that belong to no listed stage are an error.
[[nodiscard]] std::expected<std::vector<DecoderStageDescriptor>, Exception>
createStages(const std::vector<Identifier>& stageTypes, const Schema<LiteralConfigValue, Ordered>& options);

/// Creates fresh stage instances for one source. nullptr if the chain is empty.
[[nodiscard]] std::unique_ptr<Decoder> createDecoder(const std::vector<DecoderStageDescriptor>& stages);

}
