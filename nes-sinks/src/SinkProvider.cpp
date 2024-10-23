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

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks::SinkProvider
{

std::shared_ptr<Sink> lower(const QueryId queryId, const SinkDescriptor& sinkDescriptor)
{
    NES_DEBUG("The sinkDescriptor is: {}", sinkDescriptor);
    if (auto sink = SinkRegistry::instance().create(sinkDescriptor.sinkType, queryId, sinkDescriptor); sink.has_value())
    {
        return std::move(sink.value());
    }
    throw UnknownSinkType("Unknown Sink Descriptor Type: {}", sinkDescriptor.sinkType);
}

}
