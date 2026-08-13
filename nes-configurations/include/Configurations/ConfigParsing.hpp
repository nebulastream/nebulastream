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

#include <filesystem>
#include <string>
#include <vector>

#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <yaml-cpp/node/node.h>

#include <Configurations/ConfigField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>

namespace NES
{

[[nodiscard]] ConfigLiteral parseConfigLiteral(const std::string& raw);

/// Parse `--fully.qualified.key=value` arguments.
[[nodiscard]] Schema<LiteralConfigValue, Ordered> parseCommandLineConfig(const std::vector<std::string>& arguments);

/// Flatten the nested maps of a parsed YAML config into fully qualified typed config literals,
/// e.g. `worker: {query_engine: {number_of_worker_threads: 4}}` -> WORKER.QUERY_ENGINE.NUMBER_OF_WORKER_THREADS.
[[nodiscard]] Schema<LiteralConfigValue, Ordered> flattenYAMLConfig(const YAML::Node& config);

/// Load `configFile` and flatten it; load and parse failures become CannotLoadConfig carrying the path.
[[nodiscard]] Schema<LiteralConfigValue, Ordered> flattenYAMLConfig(const std::filesystem::path& configFile);

/// One named source of config literals for mergeConfigLayers, e.g. "command line" or "topology".
struct ConfigLayer
{
    std::string name;
    Schema<LiteralConfigValue, Ordered> literals;
};

/// A value replaced during mergeConfigLayers: `appliedValue` (from the higher-priority layer)
/// replaced `overwrittenValue`. Recorded even when the values are equal: setting a name in more
/// than one layer is the conflict, regardless of whether the values agree.
struct ConfigOverwrite
{
    QualifiedIdentifier name;
    std::string overwrittenLayer;
    ConfigLiteral overwrittenValue;
    std::string appliedLayer;
    ConfigLiteral appliedValue;
};

/// Build a literal schema, throwing an exception when fully qualified names collide.
[[nodiscard]] Schema<LiteralConfigValue, Ordered> createConfigLiteralSchema(std::vector<LiteralConfigValue> values);

struct ConfigMergeResult
{
    Schema<LiteralConfigValue, Ordered> literals;
    std::vector<ConfigOverwrite> overwrites;
};

/// Merge named layers, lowest priority first: a later layer replaces earlier values with the same
/// fully qualified name (exact matching); every replacement (including same-valued config values) is recorded in the result, the caller must decide
/// whether overwriting is permitted.
[[nodiscard]] ConfigMergeResult mergeConfigLayers(const std::vector<ConfigLayer>& layersLowestPriorityFirst);

}
