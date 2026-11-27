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
#include <string>
#include <unordered_map>
#include <vector>

#include <Codecs/DecoderDescriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// The decoders defined with CREATE DECODER, by name; sources resolve SOURCE.DECODERS against it.
class DecoderCatalog
{
public:
    /// Fails with UnknownDecoderType, DecoderAlreadyExists, or InvalidConfigParameter for options outside the codec's schema.
    std::expected<DecoderDescriptor, Exception>
    addDecoder(Identifier name, const Identifier& type, const std::unordered_map<Identifier, std::string>& options);

    [[nodiscard]] std::optional<DecoderDescriptor> getDecoder(const Identifier& name) const;

    /// Sources that reference the decoder keep their copy of its descriptor.
    std::optional<DecoderDescriptor> removeDecoder(const Identifier& name);

    [[nodiscard]] std::vector<DecoderDescriptor> getAllDecoders() const;

private:
    mutable std::mutex mutex;
    std::unordered_map<Identifier, DecoderDescriptor> decoders;
};

}
