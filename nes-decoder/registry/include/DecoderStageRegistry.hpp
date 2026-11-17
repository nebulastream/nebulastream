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

#include <functional>
#include <memory>
#include <string>

#include <Decoders/DecoderStage.hpp>
#include <Decoders/DecoderStageDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Maps a decoder stage name (an entry of SOURCE.DECODERS) to a factory that creates the stage from its
/// descriptor. Every source gets its own instances, since stages keep per-stream state.
using DecoderStageFactoryFn = std::function<std::unique_ptr<DecoderStage>(const DecoderStageDescriptor&)>;

class DecoderStageRegistry : public RuntimeRegistry<DecoderStageRegistry, std::string, DecoderStageFactoryFn, /*CaseSensitive*/ false>
{
public:
    static DecoderStageRegistry& instance();
};

/// The descriptor's type-erased config is the stage's own config struct (put there by its
/// DecoderStageConfigRegistry entry), so the any_cast is safe.
template <typename StageImpl, typename ConfigStruct>
DecoderStageFactoryFn makeDecoderStageFactory()
{
    return [](const DecoderStageDescriptor& descriptor) -> std::unique_ptr<DecoderStage>
    { return std::make_unique<StageImpl>(descriptor.getConfig().getAs<ConfigStruct>()); };
}

}
