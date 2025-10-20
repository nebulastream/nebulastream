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
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Sinks/SinkDescriptor.hpp>
#include <folly/Synchronized.h>
#include "DataTypes/UnboundSchema.hpp"

namespace NES
{
class SinkCatalog
{
public:
    std::optional<SinkDescriptor> addSinkDescriptor(
        Identifier sinkName, const UnboundSchema& schema, std::string_view sinkType, std::unordered_map<std::string, std::string> config);

    std::optional<SinkDescriptor> getSinkDescriptor(const Identifier& sinkName) const;

    bool removeSinkDescriptor(const Identifier& sinkName);
    bool removeSinkDescriptor(const SinkDescriptor& sinkDescriptor);

    bool containsSinkDescriptor(const Identifier& sinkName) const;
    bool containsSinkDescriptor(const SinkDescriptor& sinkDescriptor) const;

    std::vector<SinkDescriptor> getAllSinkDescriptors() const;

private:
    folly::Synchronized<std::unordered_map<Identifier , SinkDescriptor>> sinks;
};
}
