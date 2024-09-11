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

#include <Sources/Registry/GeneratedSourceRegistrar.hpp>
#include <Sources/Registry/SourceRegistry.hpp>

namespace NES::Sources
{

SourceRegistry::SourceRegistry()
{
    GeneratedSourceRegistrar::registerAllPlugins(*this);
}

std::optional<std::unique_ptr<Source>>
SourceRegistry::tryCreate(const std::string& name, const Schema& schema, const SourceDescriptor& sourceDescriptor) const
{
    if (registry.contains(name))
    {
        return {registry.at(name)(schema, sourceDescriptor)};
    }
    std::cerr << "Data source " << name << " not found.\n";
    return std::nullopt;
}

bool SourceRegistry::contains(const std::string& name) const
{
    return registry.contains(name);
}

SourceRegistry& SourceRegistry::instance()
{
    static SourceRegistry instance;
    return instance;
}

}