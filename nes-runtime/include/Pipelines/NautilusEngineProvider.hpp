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

#include <Engine.hpp>
#include <options.hpp>

namespace NES
{

/// Hands out thread-local nautilus engines so that all pipelines started on a given worker thread share a
/// single engine -- reusing its tracing Arena/ArenaPool across compilations -- instead of allocating one
/// engine per pipeline.
///
/// nautilus' createModule()/compile() are not thread-safe (they mutate a shared Arena), so an engine must
/// never be used by more than one thread. Returning a thread-local engine guarantees that: a worker thread
/// only ever drives the engine it owns, and it starts pipelines serially. Engines are cached per distinct
/// option set (compilation on/off, dump configuration, ...), which in practice is a tiny, bounded number.
///
/// Reuse is safe because a compiled executable is self-contained: the MLIR backend hands back an executable
/// that owns its own LLJIT (and thus its JIT-compiled machine code), independent of the engine. Compiling
/// the next pipeline on the same engine therefore never disturbs an already-produced executable, and an
/// executable may freely outlive (or be outlived by) the engine that produced it.
class NautilusEngineProvider
{
public:
    /// Returns a reference to the calling thread's engine for these options, creating it on first use.
    /// The reference is only valid for the duration of compilation on the calling thread.
    [[nodiscard]] static nautilus::engine::NautilusEngine& getEngine(const nautilus::engine::Options& options);
};

}
