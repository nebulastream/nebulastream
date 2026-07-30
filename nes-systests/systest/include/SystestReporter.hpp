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
#include <filesystem>
#include <functional>
#include <vector>

#include <SystestResolver.hpp>
#include <SystestRun.hpp>

namespace NES::Systest
{

class ConsoleRunReporter final : public RunReporter
{
public:
    ConsoleRunReporter(const ResolvedRun& run, bool showPerformance);

    std::expected<void, ReportingDiagnostic> publish(const RunEvent& event) override;

private:
    const ResolvedRun& run;
    bool showPerformance;
    size_t completed = 0;
    size_t total = 0;
};

class BenchmarkRunReporter final : public RunReporter
{
public:
    BenchmarkRunReporter(const ResolvedRun& run, std::filesystem::path outputFile);

    std::expected<void, ReportingDiagnostic> publish(const RunEvent& event) override;

private:
    const ResolvedRun& run;
    std::filesystem::path outputFile;
    std::vector<ValidatedResult> results;
};

class CompositeRunReporter final : public RunReporter
{
public:
    explicit CompositeRunReporter(std::vector<std::reference_wrapper<RunReporter>> reporters);

    std::expected<void, ReportingDiagnostic> publish(const RunEvent& event) override;

private:
    std::vector<std::reference_wrapper<RunReporter>> reporters;
};

}
