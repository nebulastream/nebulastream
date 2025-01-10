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

#include "VideoSource.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

#include "MQTTPlaybackVideoSourceService.hpp"
#include "VideoSourceValidation.hpp"

namespace NES
{

template <typename Service>
VideoSource<Service>::VideoSource(const SourceDescriptor& descriptor) : sourceDescriptor(descriptor)
{
    NES_TRACE("VideoSource::VideoSource: Init VideoSource.");
}


template <typename Service>
std::ostream& VideoSource<Service>::toString(std::ostream& str) const
{
    str << "\nVideoSource(";
    str << ")\n";
    return str;
}

template <typename Service>
void VideoSource<Service>::open(std::shared_ptr<AbstractBufferProvider> bufferProvider, AsyncSourceEmit&& emitFunction)
{
    service = Service::instance(sourceDescriptor, std::move(bufferProvider));
    handle = service->addConsumer(std::move(emitFunction));
}

template <typename Service>
void VideoSource<Service>::close()
{
    if (handle)
    {
        service->removeConsumer(*handle);
    }
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterVideoSource(SourceRegistryArguments sourceRegistryArguments) ///NOLINT
{
    return std::make_unique<VideoSource<VideoSourceService>>(sourceRegistryArguments.sourceDescriptor);
}

SourceRegistryReturnType
SourceGeneratedRegistrar::RegisterMQTTVideoPlaybackSource(SourceRegistryArguments sourceRegistryArguments) ///NOLINT
{
    return std::make_unique<VideoSource<MQTTPlaybackVideoSourceService>>(sourceRegistryArguments.sourceDescriptor);
}

}
