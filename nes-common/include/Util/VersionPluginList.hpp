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

#include <algorithm>
#include <functional>
#include <iterator>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace NES
{

/// Collects the plugin names of all registries linked into the current binary, grouped by a human-readable kind (e.g. "Sources").
/// Registry headers register a provider at static initialization time via the VersionPluginListEntry helper below; the registries
/// themselves are only queried lazily when list() is called (from formatVersion), so `--version` reflects exactly the plugins
/// compiled into the binary without instantiating anything up front.
class VersionPluginList
{
public:
    using Provider = std::function<std::vector<std::string>()>;

    static VersionPluginList& instance()
    {
        static VersionPluginList instance;
        return instance;
    }

    void addProvider(std::string kind, Provider provider) { providers.emplace_back(std::move(kind), std::move(provider)); }

    /// Returns the plugin names per kind, sorted by kind and name. Multiple providers of the same kind (e.g. the source and
    /// source-validation registries) are merged and deduplicated; kinds without any names are omitted.
    [[nodiscard]] std::vector<std::pair<std::string, std::vector<std::string>>> list() const
    {
        std::map<std::string, std::vector<std::string>> grouped;
        for (const auto& [kind, provider] : providers)
        {
            auto providedNames = provider();
            auto& names = grouped[kind];
            names.insert(names.end(), std::make_move_iterator(providedNames.begin()), std::make_move_iterator(providedNames.end()));
        }
        std::vector<std::pair<std::string, std::vector<std::string>>> result;
        for (auto& [kind, names] : grouped)
        {
            std::ranges::sort(names);
            const auto duplicates = std::ranges::unique(names);
            names.erase(duplicates.begin(), duplicates.end());
            if (!names.empty())
            {
                result.emplace_back(kind, std::move(names));
            }
        }
        return result;
    }

private:
    VersionPluginList() = default;

    std::vector<std::pair<std::string, Provider>> providers;
};

/// Registers a provider on construction. Intended to be defined as an inline variable at the end of a registry header, so that
/// every binary linking a translation unit that uses the registry lists the registry's plugins under `--version`.
struct VersionPluginListEntry
{
    VersionPluginListEntry(std::string kind, VersionPluginList::Provider provider)
    {
        VersionPluginList::instance().addProvider(std::move(kind), std::move(provider));
    }
};

}
