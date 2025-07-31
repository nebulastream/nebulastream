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
#include <atomic>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <folly/Synchronized.h>

namespace NES
{
class SinkCatalog
{
public:
    std::optional<SinkDescriptor> addSinkDescriptor(
        Identifier sinkName, const SchemaBase<UnboundFieldBase<1>, true>& schema, const Identifier& sinkType, std::unordered_map<Identifier, std::string> config);

    std::optional<SinkDescriptor> getSinkDescriptor(const Identifier& sinkName) const;

    [[nodiscard]] std::optional<SinkDescriptor>
    getInlineSink(const SchemaBase<UnboundFieldBase<1>, true>& schema, const Identifier& sinkType, std::unordered_map<Identifier, std::string> config) const;

    bool removeSinkDescriptor(const Identifier& sinkName);
    bool removeSinkDescriptor(const SinkDescriptor& sinkDescriptor);

    bool containsSinkDescriptor(const Identifier& sinkName) const;
    bool containsSinkDescriptor(const SinkDescriptor& sinkDescriptor) const;

    std::vector<SinkDescriptor> getAllSinkDescriptors() const;

private:
    mutable std::atomic<InlineSinkId::Underlying> nextInlineSinkId{INITIAL_INLINE_SINK_ID.getRawValue()};
    folly::Synchronized<std::unordered_map<Identifier, SinkDescriptor>> sinks;
};
}
