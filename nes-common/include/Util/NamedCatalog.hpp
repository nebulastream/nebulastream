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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace NES
{

/// Generic name-keyed storage for a "register by name, resolve by name" catalog (e.g. a UDF or
/// model catalog): register/remove/has/list/load over an in-memory map. Deliberately holds no
/// validation logic and no opinion on what a "not found" error looks like -- callers own both,
/// so this stays reusable across catalogs whose entries and error types differ.
///
/// Not thread-safe -- callers are expected to serialize registration externally (e.g. through DDL
/// statement handling), the same contract every catalog built on top of this already documents.
template <typename DescriptorT>
class NamedCatalog
{
    std::unordered_map<std::string, DescriptorT> entries;

public:
    void registerEntry(std::string name, DescriptorT descriptor) { entries.insert_or_assign(std::move(name), std::move(descriptor)); }

    void removeEntry(const std::string& name) { entries.erase(name); }

    [[nodiscard]] bool hasEntry(const std::string& name) const { return entries.contains(name); }

    [[nodiscard]] std::vector<std::string> getNames() const
    {
        std::vector<std::string> names;
        names.reserve(entries.size());
        for (const auto& [name, descriptor] : entries)
        {
            names.push_back(name);
        }
        return names;
    }

    [[nodiscard]] std::vector<DescriptorT> getEntries() const
    {
        std::vector<DescriptorT> descriptors;
        descriptors.reserve(entries.size());
        for (const auto& [name, descriptor] : entries)
        {
            descriptors.push_back(descriptor);
        }
        return descriptors;
    }

    /// Returns the entry registered under `name`, or invokes `makeNotFound()` and throws its result
    /// if absent. The exception type/message stays the caller's concern (e.g. `UnknownUdf`).
    template <typename MakeNotFound>
    [[nodiscard]] DescriptorT load(const std::string& name, MakeNotFound&& makeNotFound) const
    {
        if (const auto it = entries.find(name); it != entries.end())
        {
            return it->second;
        }
        throw makeNotFound();
    }
};

}
