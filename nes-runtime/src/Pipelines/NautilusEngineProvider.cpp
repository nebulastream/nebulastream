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
#include <Pipelines/NautilusEngineProvider.hpp>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <folly/hash/Hash.h>
#include <Engine.hpp>
#include <options.hpp>

namespace NES
{

namespace
{
/// The subset of options that change how an engine compiles, and therefore distinguish cached engines.
/// nautilus reads dump.* and the strategy/backend from the engine's options at compile time, so engines
/// configured differently must not be shared.
struct EngineKey
{
    bool compilation;
    std::string backend;
    std::string strategy;
    bool mlirMultithreading;
    bool dumpAll;
    bool dumpConsole;
    bool dumpFile;
    bool dumpGraph;
    bool operator==(const EngineKey&) const = default;
};

struct EngineKeyHash
{
    std::size_t operator()(const EngineKey& key) const noexcept
    {
        return folly::hash::hash_combine(
            key.backend, key.strategy, key.compilation, key.mlirMultithreading, key.dumpAll, key.dumpConsole, key.dumpFile, key.dumpGraph);
    }
};

EngineKey keyFromOptions(const nautilus::engine::Options& options)
{
    return EngineKey{
        .compilation = options.getOptionOrDefault<bool>("engine.Compilation", true),
        .backend = options.getOptionOrDefault<std::string>("engine.backend", std::string{}),
        .strategy = options.getOptionOrDefault<std::string>("engine.compilationStrategy", std::string{}),
        .mlirMultithreading = options.getOptionOrDefault<bool>("mlir.enableMultithreading", false),
        .dumpAll = options.getOptionOrDefault<bool>("dump.all", false),
        .dumpConsole = options.getOptionOrDefault<bool>("dump.console", false),
        .dumpFile = options.getOptionOrDefault<bool>("dump.file", false),
        .dumpGraph = options.getOptionOrDefault<bool>("dump.graph", false),
    };
}
}

nautilus::engine::NautilusEngine& NautilusEngineProvider::getEngine(const nautilus::engine::Options& options)
{
    /// One cache per worker thread: an engine is only ever driven by the thread that owns it.
    static thread_local std::unordered_map<EngineKey, nautilus::engine::NautilusEngine, EngineKeyHash> engines;
    const auto key = keyFromOptions(options);
    if (const auto it = engines.find(key); it != engines.end())
    {
        return it->second;
    }
    return engines.try_emplace(key, options).first->second;
}

}
