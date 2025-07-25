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

#include <Sinks/SinkProvider.hpp>

#include <memory>
#include <utility>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>

namespace NES::Sinks::SinkProvider
{

std::unique_ptr<Sink> lower(const SinkDescriptor& sinkDescriptor)
{
    NES_DEBUG("The sinkDescriptor is: {}", sinkDescriptor);
    auto sinkArguments = NES::Sinks::SinkRegistryArguments(sinkDescriptor);
    if (auto sink = SinkRegistry::instance().create(sinkDescriptor.getSinkType(), sinkArguments); sink.has_value())
    {
        return std::move(sink.value());
    }
    throw UnknownSinkType("Unknown Sink Descriptor Type: {}", sinkDescriptor.getSinkType());
}

}
