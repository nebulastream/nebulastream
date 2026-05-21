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

namespace NES
{

/// Public facade over the `SinkCaptureRegistry`, the sink-side mirror of
/// `SourceDataProvider`. A systest that writes `SELECT ... INTO <Type>(...)`
/// for a sink type registered here is testing that sink against a real
/// external system: the registrar rewrites the sink's connection knobs to the
/// dispatcher-allocated endpoint and spawns a capture thread that drains the
/// external system into the systest result file. The sink under test stays
/// completely test-unaware — all test scaffolding lives in the registrar.
class SinkDataProvider
{
public:
    /// Whether `sinkType` has a capture registrar. The systest binder calls
    /// this to decide whether a `SELECT ... INTO <sinkType>(...)` is an
    /// ordinary sink or an external sink under test.
    static bool hasCaptureFor(const std::string& sinkType);

    /// Set up capture for an external sink under test. Appends the capture
    /// subscriber to `serverThreads` (the shared per-test-file thread vector)
    /// and returns the sink config with its connection knobs rewritten to the
    /// dispatcher endpoint, ready for the binder to build the real sink
    /// descriptor from. A pull-based capture additionally stores a result
    /// finalizer in `resultFinalizer` for the runner to invoke once the query
    /// has stopped; see SinkCaptureRegistryArguments::resultFinalizer.
    static std::unordered_map<std::string, std::string> provideSinkCapture(
        const std::string& sinkType,
        std::unordered_map<std::string, std::string> sinkConfig,
        std::filesystem::path resultFile,
        std::shared_ptr<std::vector<std::jthread>> serverThreads,
        std::shared_ptr<std::function<void()>> resultFinalizer);
};

}
