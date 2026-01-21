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

#include <Encoders/EncoderProvider.hpp>

#include <memory>
#include <string>
#include <utility>
#include <Encoders/Encoder.hpp>
#include <EncoderRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::EncoderProvider
{
std::unique_ptr<Encoder> provideEncoder(std::string encoderType)
{
    auto encoderArguments = EncoderRegistryArguments();
    if (auto encoder = EncoderRegistry::instance().create(encoderType, encoderArguments))
    {
        return std::move(encoder.value());
    }
    throw UnknownEncoderType("Unknown encoder type: {}", encoderType);
}
}
