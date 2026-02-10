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
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <Configurations/OptionVisitor.hpp>

namespace NES
{

/// Visitor that prints configuration values with their full dotted paths.
/// Output format: `section.subsection.option: value` with `(default)` suffix for unset options.
///
/// Example output:
///   worker.query_engine.number_of_worker_threads: 8
///   worker.query_engine.admission_queue_size: 1000 (default)
///   grpc: [::]:8080 (default)
///   connection: sink-node:9090
class ConfigValuePrinter final : public OptionVisitor
{
public:
    explicit ConfigValuePrinter(std::ostream& ostream) : os(ostream) { }

    void pushSection(std::string_view sectionName) override { path.emplace_back(sectionName); }
    void popSection() override { path.pop_back(); }

    void push() override { }
    void pop() override { }

    void visitOption(const OptionInfo& info) override
    {
        if (info.currentValue.empty())
        {
            return; /// Skip non-leaf configuration sections
        }
        for (const auto& segment : path)
        {
            os << segment << ".";
        }
        os << info.name << ": " << info.currentValue;
        if (!info.isExplicitlySet)
        {
            os << " (default)";
        }
        os << "\n";
    }

private:
    std::vector<std::string> path;
    std::ostream& os;
};
}
