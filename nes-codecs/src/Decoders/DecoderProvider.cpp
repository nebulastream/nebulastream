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

#include <Decoders/DecoderProvider.hpp>

#include <memory>
#include <string>
#include <Decoders/Decoder.hpp>
#include <DecoderRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::DecoderProvider
{

std::unique_ptr<Decoder> provideDecoder(std::string decoderType)
{
    auto decoderArguments = DecoderRegistryArguments();
    /// Try to create a decoder of the decoderType
    if (auto decoder = DecoderRegistry::instance().create(decoderType, decoderArguments))
    {
        return std::move(decoder.value());
    }
    throw UnknownDecoderType("Unknown decoder type: {}", decoderType);
}
}
