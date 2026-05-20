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

#include <IreeCompiler.hpp>

#include <expected>
#include <string>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <BackendTool.hpp>
#include <Inference.hpp>
#include <Model.hpp>
#include <ModelAccess.hpp>

namespace NES
{

IreeCompiler::IreeCompiler()
{
    discovery.name = "iree-compile";
    auto found = detail::discoverTool(discovery.name, /*checkVersion*/ true);
    if (!found.has_value())
    {
        NES_WARNING("{}: {}", discovery.name, found.error());
        return;
    }
    discovery = std::move(*found);
    if (!discovery.version.has_value())
    {
        return;
    }
    if (const auto& version = discovery.version.value(); version != expectedIreeVersion)
    {
        NES_WARNING("{}: version mismatch (got {}, expected {})", discovery.name, format_as(version), format_as(expectedIreeVersion));
        return;
    }
    NES_INFO("{}", detail::format_as(discovery));
}

std::expected<CompiledModel, CompileError> IreeCompiler::compile(const ImportedModel& imported) const
{
    if (!available())
    {
        return std::unexpected(CompileError{"iree-compile is not available"});
    }
    if (imported.empty())
    {
        return std::unexpected(CompileError{"ImportedModel is empty"});
    }

    std::vector<std::string> args;
    args.emplace_back("-");
    args.emplace_back("--iree-hal-target-device=local");
    args.emplace_back("--iree-hal-local-target-device-backends=llvm-cpu");
    args.emplace_back("--iree-llvmcpu-debug-symbols=false");
    args.emplace_back("--iree-stream-partitioning-favor=min-peak-memory");
    args.emplace_back("--iree-llvmcpu-target-cpu=host");

    auto result = detail::runTool(discovery.path, args, imported.getData());
    if (!result.errorMessage.empty())
    {
        NES_ERROR("iree-compile launch error: {}", result.errorMessage);
        return std::unexpected(CompileError{fmt::format("Model compile failed: {}", result.errorMessage)});
    }
    if (result.exitCode != 0)
    {
        NES_ERROR("iree-compile error:\n{} {}\n```\n{}```", discovery.path.string(), fmt::join(args, " "), result.stderrText);
        return std::unexpected(CompileError{"Model compile was not successful: Non Zero Exit Code."});
    }

    return detail::ModelAccess::compileFrom(imported, detail::RefCountedByteBuffer::fromBytes(result.stdoutData));
}

}
