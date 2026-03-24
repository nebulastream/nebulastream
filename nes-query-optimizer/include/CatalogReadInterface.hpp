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

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

struct CatalogRef;

namespace CapacityKind
{
struct Unlimited
{
};

struct Limited
{
    size_t value;
};
}

using Capacity = std::variant<CapacityKind::Unlimited, CapacityKind::Limited>;

struct WorkerInfo
{
    Host host; /// gRPC management endpoint
    std::string data; /// Data-plane address for network sources/sinks
    Capacity maxOperators;
};

std::optional<LogicalSource> getLogicalSource(const CatalogRef& ctx, std::string_view name);

std::optional<std::unordered_set<SourceDescriptor>> getSourceDescriptors(const CatalogRef& ctx, std::string_view logicalSourceName);

std::optional<SinkDescriptor> getSinkDescriptor(const CatalogRef& ctx, std::string_view name);

std::optional<WorkerInfo> getWorker(const CatalogRef& ctx, const Host& host);

/// Factory functions for inline (query-defined) sources and sinks.
/// These are pure C++ operations (config validation + construction), no catalog lookup.
std::optional<SourceDescriptor> createInlineSource(
    const std::string& sourceType,
    const Schema& schema,
    std::unordered_map<std::string, std::string> parserConfigMap,
    std::unordered_map<std::string, std::string> sourceConfigMap);

std::optional<SinkDescriptor> createInlineSink(
    const Schema& schema,
    std::string_view sinkType,
    std::unordered_map<std::string, std::string> config,
    const std::unordered_map<std::string, std::string>& formatConfig);

}
