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

#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkGeneratedRegistrar.hpp>
#include <Sinks/SinkRegistry.hpp>

namespace NES::Sinks
{

SinkRegistry::SinkRegistry()
{
    SinkGeneratedRegistrar::registerAllPlugins(*this);
}

std::optional<std::unique_ptr<Sink>>
SinkRegistry::tryCreate(const std::string& name, const QueryId queryId, const SinkDescriptor& sinkDescriptor) const
{
    if (registry.contains(name))
    {
        return {registry.at(name)(queryId, sinkDescriptor)};
    }
    std::cerr << "Data sink " << name << " not found.\n";
    return std::nullopt;
}

SinkRegistry& SinkRegistry::instance()
{
    static SinkRegistry instance;
    return instance;
}

}
