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

#include <string>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksValidation/SinkGeneratedRegistrarValidation.hpp>
#include <SinksValidation/SinkRegistryValidation.hpp>

namespace NES::Sinks
{

SinkRegistryValidation::SinkRegistryValidation()
{
    SinkGeneratedRegistrarValidation::registerAllPlugins(*this);
}

std::optional<Configurations::DescriptorConfig::Config>
SinkRegistryValidation::tryCreate(const std::string& name, std::unordered_map<std::string, std::string>&& sinkConfig) const
{
    if (registry.contains(name))
    {
        return {registry.at(name)(std::move(sinkConfig))};
    }
    std::cerr << "Data sink " << name << " not found.\n";
    return std::nullopt;
}

bool SinkRegistryValidation::contains(const std::string& name) const
{
    return registry.contains(name);
}

SinkRegistryValidation& SinkRegistryValidation::instance()
{
    static SinkRegistryValidation instance;
    return instance;
}

}
