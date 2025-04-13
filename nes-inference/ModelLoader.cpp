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

#include <Model.hpp>
#include <ModelLoader.hpp>

#include <array>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <ios>
#include <iterator>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <boost/process.hpp>
#include <boost/process/search_path.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace bp = boost::process;
using namespace std::literals;

namespace NES::Nebuli::Inference
{

namespace
{
struct Tool
{
    Tool(const std::string_view& name, bool hasVersion) : name(name), hasVersion(hasVersion) { }
    std::string_view name;
    bool hasVersion = false;
    bool available = false;
    std::string version;
};

auto format_as(const Tool& tool)
{
    if (tool.available)
    {
        if (tool.hasVersion)
        {
            return fmt::format("{}: {}", tool.name, tool.version);
        }
        else
        {
            return fmt::format("{}: available", tool.name);
        }
    }
    else
    {
        return fmt::format("{}: Not Found", tool.name);
    }
}

bool checkInferenceToolsAreAvailable()
{
    std::array tools = {Tool{"iree-import-onnx"sv, false}, Tool{"iree-compile"sv, true}};
    for (auto& tool : tools)
    {
        auto binaryInPath = bp::search_path(std::string(tool.name));
        if (binaryInPath.empty())
        {
            NES_WARNING("{} is not in PATH", tool.name);
            continue;
        }
        tool.available = true;

        if (tool.hasVersion)
        {
            try
            {
                // Create a child process that executes 'command --version'
                // Redirect stdout to null to avoid printing output
                bp::ipstream pipe_stream;
                bp::child c(binaryInPath, "--version", bp::std_out > pipe_stream);

                // Read the output
                std::string line;
                while (pipe_stream && std::getline(pipe_stream, line))
                {
                    tool.version += line + "\n";
                }

                // Wait for the process to finish
                c.wait();
            }
            catch (const bp::process_error& bpe)
            {
                NES_WARNING("Could not retrieve version of '{}':\n{}", tool.name, bpe.what());
                tool.available = false;
            }
        }
    }

    if (!std::ranges::all_of(tools, &Tool::available))
    {
        NES_WARNING(
            "Missing Inference Tools:\n{}", fmt::join(std::views::filter(tools, [](const auto& tool) { return !tool.available; }), "\n"));
        return false;
    }
    NES_INFO("Installed Inference Tools:\n{}", fmt::join(std::views::filter(tools, &Tool::available), "\n"));
    return true;
}

}

bool enabled()
{
#ifndef NEBULI_INFERENCE_SUPPORT
    return false;
#else
    static bool isAvailable = checkInferenceToolsAreAvailable();
    return isAvailable;
#endif
}

std::expected<Model, ModelLoadError> load(const std::filesystem::path& modelPath, const ModelOptions& options)
{
    std::vector<std::string> importArgs;
    std::vector<std::string> compileArgs;

    if (modelPath.filename().extension() != ".onnx")
    {
        return std::unexpected(ModelLoadError{"Loading does only support `.onnx` models at the moment"});
    }

    importArgs.emplace_back(modelPath);

    if (auto opset = options.opset)
    {
        importArgs.emplace_back("--opset-version");
        importArgs.emplace_back(std::to_string(*opset));
    }

    /// Pipe output of import to compiler
    compileArgs.emplace_back("-");
    compileArgs.emplace_back("--iree-hal-target-device=local");
    compileArgs.emplace_back("--iree-hal-local-target-device-backends=llvm-cpu");

    ///TODO: (#???) This only works if nebuli and the worker are running on the same arch
    compileArgs.emplace_back("--iree-llvmcpu-target-cpu=host");

    try
    {
        bp::pipe mlir_pipe;
        bp::ipstream model_stream;
        std::vector<bp::child> process;
        process.emplace_back(bp::search_path("iree-import-onnx"), importArgs, bp::std_out > mlir_pipe);
        process.emplace_back(bp::search_path("iree-compile"), compileArgs, bp::std_in<mlir_pipe, bp::std_out> model_stream);

        /// Read output of iree-compile into a byte buffer
        std::vector<std::byte> modelVmfb;
        std::array<std::byte, 4096> buffer;

        while (process[1].running() && model_stream.good())
        {
            model_stream.read(reinterpret_cast<char*>(buffer.data()), buffer.size());

            const std::streamsize count = model_stream.gcount();
            if (count <= 0)
            {
                break;
            }

            const std::span bytesWritten = {buffer.data(), static_cast<size_t>(count)};
            // Append buffer contents to our vector
            std::ranges::copy(bytesWritten, std::back_inserter(modelVmfb));
        }

        std::ranges::for_each(process, [](auto& process) { process.wait(); });
        const auto success = std::ranges::all_of(process, [](const auto& process) { return process.exit_code() == 0; });
        if (success)
        {
            auto modelVmfb1 = std::make_shared<std::byte[]>(modelVmfb.size());
            auto modelVmfb1Span = std::span{modelVmfb1.get(), modelVmfb.size()};
            std::ranges::copy(modelVmfb, modelVmfb1Span.begin());
            return Model{std::move(modelVmfb1), modelVmfb.size()};
        }
        return std::unexpected(ModelLoadError("Model Import was not successful: Non Zero Exit Code."));
    }
    catch (bp::process_error& bpe)
    {
        return std::unexpected(ModelLoadError(fmt::format("Model Import was not successful: {}", bpe.what())));
    }
}
}