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
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Util/Registry.hpp>

namespace NES
{

/// The (possibly endpoint-rewritten) sink config the registrar hands back to
/// the systest binder, which then builds the real sink descriptor from it.
using SinkCaptureRegistryReturnType = std::unordered_map<std::string, std::string>;

/// Arguments handed to a per-sink-type capture registrar. The mirror image of
/// `InlineDataRegistryArguments` on the source side: where the source registrar
/// spawns a publisher that feeds the external system, the sink capture
/// registrar spawns a subscriber that drains the external system into the
/// systest result file so the normal `----` comparison can run.
struct SinkCaptureRegistryArguments
{
    /// The `INTO <Type>(...)` config of the sink under test. The registrar
    /// rewrites the connection knobs (e.g. the broker endpoint) to the
    /// dispatcher-allocated host port and returns the result.
    std::unordered_map<std::string, std::string> sinkConfig;
    /// Path the capture appends the drained tuples to. The schema header line a
    /// File sink would write first is emitted separately by the systest binder
    /// once schema inference has run, so the capture writes only the body.
    std::filesystem::path resultFile;
    /// The same per-test-file thread vector the source publisher threads live
    /// in; the registrar appends its capture subscriber here. Joined when the
    /// test file is done -- a push-based capture writes the result file eagerly
    /// as messages arrive, so it needs no dedicated join before `checkResult`.
    std::shared_ptr<std::vector<std::jthread>> serverThreads;
    /// Out-slot for a pull-based capture. Where a push-based capture (MQTT)
    /// drains the external system into `resultFile` as the query runs, a
    /// pull-based one (ODBC) has to read the system back -- and a read taken
    /// while the query is still running races the rows still in flight. Such a
    /// registrar leaves a closure here instead of spawning a polling thread;
    /// the systest runner invokes it exactly once, after the query has reached
    /// Stopped -- so every written row is settled -- and before the result
    /// check opens `resultFile`. A push-based capture leaves this unset.
    std::shared_ptr<std::function<void()>> resultFinalizer;
};

class SinkCaptureRegistry
    : public BaseRegistry<SinkCaptureRegistry, std::string, SinkCaptureRegistryReturnType, SinkCaptureRegistryArguments>
{
};

}

#define INCLUDED_FROM_SINKS_SINK_CAPTURE_REGISTRY
#include <SinkCaptureGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SINKS_SINK_CAPTURE_REGISTRY
