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
#include <Sources/SourceDescriptor.hpp>
#include <SourcesValidation/GeneratedRegistrarSourceValidation.hpp>
#include <SourcesValidation/RegistrySourceValidation.hpp>

namespace NES::Sources
{

RegistrySourceValidation::RegistrySourceValidation()
{
    GeneratedRegistrarSourceValidation::registerAllPlugins(*this);
}

std::optional<SourceDescriptor::Config>
RegistrySourceValidation::tryCreate(const std::string& name, std::map<std::string, std::string>&& sourceConfig) const
{
    if (registry.contains(name))
    {
        return {registry.at(name)(std::move(sourceConfig))};
    }
    std::cerr << "Data source " << name << " not found.\n";
    return std::nullopt;
}

bool RegistrySourceValidation::contains(const std::string& name) const
{
    return registry.contains(name);
}

RegistrySourceValidation& RegistrySourceValidation::instance()
{
    static RegistrySourceValidation instance;
    return instance;
}

}
