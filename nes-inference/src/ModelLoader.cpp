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

#include <ModelLoader.hpp>

#include <Model.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <numeric>
#include <optional>
#include <ranges>
#include <regex>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/process/v2/environment.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <scope_guard.hpp>

namespace bpv2 = boost::process::v2;
namespace asio = boost::asio;
using namespace std::literals;

namespace NES::Inference
{

namespace
{

constexpr size_t pipeBufferSize = 4096;
constexpr auto importTimeout = std::chrono::seconds(10);
constexpr auto compileTimeout = std::chrono::seconds(20);

struct Tool
{
    Tool(const std::string_view& name, bool hasVersion) : name(name), hasVersion(hasVersion) { }

    std::string_view name;
    bool hasVersion = false;
    bool available = false;
    std::string version;
};

struct ModelMetadataGraph
{
    struct VertexProps
    {
        std::string label;
        std::string shape;
    };

    struct ModelMetadata
    {
        std::vector<size_t> inputShape;
        std::vector<size_t> outputShape;
        std::string functionName;
    };

    using Graph = boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS, VertexProps>;
    Graph graph;

    explicit ModelMetadataGraph(const std::string& dotFilePath)
    {
        boost::dynamic_properties dynProps = boost::dynamic_properties(boost::ignore_other_properties);
        dynProps.property("label", boost::get(&VertexProps::label, graph));
        dynProps.property("shape", boost::get(&VertexProps::shape, graph));

        std::ifstream dotFile(dotFilePath);
        if (!dotFile.is_open())
        {
            throw CannotLoadModel("Could not open DOT graph file: {}", dotFilePath);
        }
        boost::read_graphviz(dotFile, graph, dynProps);
    }

    static std::vector<size_t> parseTensorShape(const std::string& label)
    {
        /// Match tensor shapes with any element type (e.g., f32, f64, i32)
        static const std::regex tensorRegex(R"(tensor<([?0-9x]+)([a-z][a-z0-9]+)>)");
        std::smatch match;
        std::vector<size_t> result;
        if (std::regex_search(label, match, tensorRegex))
        {
            std::string elementType = match[2];
            if (elementType != "f32")
            {
                throw CannotLoadModel("Unsupported tensor element type '{}' in label '{}'. Only f32 is supported.", elementType, label);
            }
            const std::string shapeStr = match[1];
            std::stringstream ss(shapeStr);
            std::string dim;
            while (std::getline(ss, dim, 'x'))
            {
                if (dim == "?")
                {
                    result.push_back(1);
                }
                else
                {
                    result.push_back(std::stoi(dim));
                }
            }
        }
        return result;
    }

    static std::string parseFunctionName(const std::string& label)
    {
        static const std::regex graphNameRegex(R"(@([a-zA-Z0-9_]+)\$)");
        std::smatch match;
        if (std::regex_search(label, match, graphNameRegex))
        {
            return match[1];
        }
        return {};
    }

    /// Function to get next node label if there's exactly one outgoing edge
    std::optional<std::string> findLabelOfUnambiguousSuccessor(auto vertex, const auto& graphRef)
    {
        auto [adjIt, adjEnd] = adjacent_vertices(vertex, graphRef);
        if (adjIt == adjEnd)
        {
            return std::nullopt;
        }

        const auto nextVertex = *adjIt;
        ++adjIt;
        if (adjIt != adjEnd)
        {
            return std::nullopt;
        }

        return graphRef[nextVertex].label;
    }

    ModelMetadata getModelMetadata()
    {
        ModelMetadata metadata;
        for (auto vertex : boost::make_iterator_range(boost::vertices(graph)))
        {
            const std::string& label = graph[vertex].label;

            if (label.find("hal.tensor.import") != std::string::npos)
            {
                metadata.inputShape = parseTensorShape(label);
            }
            else if (label.find("flow.dispatch") != std::string::npos)
            {
                metadata.functionName = parseFunctionName(label);
            }
            else if (findLabelOfUnambiguousSuccessor(vertex, graph)
                         .transform([](const auto& label) { return label.find("hal.tensor.export") != std::string::npos; })
                         .value_or(false))
            {
                metadata.outputShape = parseTensorShape(label);
            }
        }
        if (metadata.inputShape.empty() || metadata.outputShape.empty() || metadata.functionName.empty())
        {
            throw CannotLoadModel("DOT graph is missing required model metadata (input shape, output shape, or function name)");
        }
        return metadata;
    }
};

auto format_as(const Tool& tool)
{
    if (tool.available)
    {
        if (tool.hasVersion)
        {
            return fmt::format("{}: {}", tool.name, tool.version);
        }
        return fmt::format("{}: available", tool.name);
    }
    return fmt::format("{}: Not Found", tool.name);
}

/// Read all available data from a readable_pipe synchronously until EOF.
std::string readAllFromPipe(asio::readable_pipe& pipe)
{
    std::string result;
    boost::system::error_code errCode;
    std::array<char, pipeBufferSize> buf{};
    for (;;)
    {
        auto bytesRead = pipe.read_some(asio::buffer(buf), errCode);
        if (bytesRead > 0)
        {
            result.append(buf.data(), bytesRead);
        }
        if (errCode)
        {
            break;
        }
    }
    return result;
}

bool checkInferenceToolsAreAvailable()
{
    std::array tools = {Tool{"iree-import-onnx"sv, false}, Tool{"iree-compile"sv, true}};
    for (auto& tool : tools)
    {
        auto binaryInPath = bpv2::environment::find_executable(std::string(tool.name));
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
                asio::io_context ctx;
                asio::readable_pipe stdoutPipe(ctx);
                const std::vector<std::string> args = {"--version"};
                /// NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection) false positive: boost process v2 fork internals
                bpv2::process proc(ctx, binaryInPath, args, bpv2::process_stdio{.in = {}, .out = stdoutPipe, .err = {}});

                tool.version = readAllFromPipe(stdoutPipe);
                proc.wait();
            }
            catch (const boost::system::system_error& e)
            {
                NES_WARNING("Could not retrieve version of '{}':\n{}", tool.name, e.what());
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
    static const bool isAvailable = checkInferenceToolsAreAvailable();
    return isAvailable;
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

    compileArgs.emplace_back("-");
    compileArgs.emplace_back("--iree-hal-target-device=local");
    compileArgs.emplace_back("--iree-hal-local-target-device-backends=llvm-cpu");
    compileArgs.emplace_back("--iree-llvmcpu-debug-symbols=false");
    compileArgs.emplace_back("--iree-stream-partitioning-favor=min-peak-memory");
    compileArgs.emplace_back("--iree-llvmcpu-target-cpu=host");

    static std::atomic<uint64_t> loadCounter{0};
    auto graphDir
        = std::filesystem::temp_directory_path() / ("nes-graph-" + std::to_string(getpid()) + "-" + std::to_string(loadCounter++));
    std::filesystem::create_directories(graphDir);
    SCOPE_EXIT
    {
        std::filesystem::remove_all(graphDir);
    };
    const std::string graphPath = graphDir.string() + "/model.dot";
    compileArgs.emplace_back("--iree-flow-dump-dispatch-graph");
    compileArgs.emplace_back("--iree-flow-dump-dispatch-graph-output-file=" + graphPath);

    try
    {
        asio::io_context ctx;

        /// Create the inter-process pipe (import stdout -> compile stdin).
        /// We create a raw OS pipe and use the read-end as the compile process's stdin
        /// and the write-end as the import process's stdout.
        asio::readable_pipe mlirPipeRead(ctx);
        asio::writable_pipe mlirPipeWrite(ctx);
        asio::connect_pipe(mlirPipeRead, mlirPipeWrite);

        /// Pipes for capturing stderr of each process.
        asio::readable_pipe importStderrPipe(ctx);
        asio::readable_pipe compileStderrPipe(ctx);

        /// Pipe for capturing the compiled model output (stdout of iree-compile).
        asio::readable_pipe modelOutputPipe(ctx);

        auto importExe = bpv2::environment::find_executable("iree-import-onnx");
        auto compileExe = bpv2::environment::find_executable("iree-compile");

        /// Launch iree-import-onnx: stdout -> mlirPipeWrite, stderr -> importStderrPipe
        /// NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection) false positive: boost process v2 fork internals
        bpv2::process importProc(ctx, importExe, importArgs, bpv2::process_stdio{.in = {}, .out = mlirPipeWrite, .err = importStderrPipe});

        /// Close our copy of the write-end so iree-compile sees EOF when import finishes.
        mlirPipeWrite.close();

        /// Launch iree-compile: stdin <- mlirPipeRead, stdout -> modelOutputPipe, stderr -> compileStderrPipe
        /// NOLINTNEXTLINE(clang-analyzer-unix.BlockInCriticalSection) false positive: boost process v2 fork internals
        bpv2::process compileProc(
            ctx, compileExe, compileArgs, bpv2::process_stdio{.in = mlirPipeRead, .out = modelOutputPipe, .err = compileStderrPipe});

        /// Close our copy of the read-end; iree-compile owns it now.
        mlirPipeRead.close();

        /// Read the compiled model bytecode from iree-compile's stdout.
        std::vector<std::byte> modelVmfb;
        {
            boost::system::error_code errCode;
            std::array<std::byte, pipeBufferSize> buffer{};
            for (;;)
            {
                auto bytesRead = modelOutputPipe.read_some(asio::buffer(buffer), errCode);
                if (bytesRead > 0)
                {
                    std::ranges::copy(std::span{buffer.data(), bytesRead}, std::back_inserter(modelVmfb));
                }
                if (errCode)
                {
                    break;
                }
            }
        }

        /// Use async_wait with steady_timers for timeout, avoiding the polling loop race condition.
        bool importDone = false;
        bool compileDone = false;
        int importExitCode = -1;
        int compileExitCode = -1;
        bool importTimedOut = false;
        bool compileTimedOut = false;

        asio::steady_timer importTimer(ctx, importTimeout);
        importTimer.async_wait(
            [&](const boost::system::error_code& errCode)
            {
                if (!errCode)
                {
                    /// Import timer expired before the import process finished.
                    importTimedOut = true;
                    boost::system::error_code termErrCode;
                    importProc.terminate(termErrCode);
                    compileProc.terminate(termErrCode);
                }
            });

        asio::steady_timer compileTimer(ctx, compileTimeout);
        compileTimer.async_wait(
            [&](const boost::system::error_code& errCode)
            {
                if (!errCode)
                {
                    /// Compile timer expired before the compile process finished.
                    compileTimedOut = true;
                    boost::system::error_code termErrCode;
                    compileProc.terminate(termErrCode);
                }
            });

        importProc.async_wait(
            [&](boost::system::error_code /*ec*/, int exitCode)
            {
                importDone = true;
                importExitCode = exitCode;
                importTimer.cancel();
                if (importDone && compileDone)
                {
                    compileTimer.cancel();
                }
            });

        compileProc.async_wait(
            [&](boost::system::error_code /*ec*/, int exitCode)
            {
                compileDone = true;
                compileExitCode = exitCode;
                compileTimer.cancel();
                if (importDone && compileDone)
                {
                    importTimer.cancel();
                }
            });

        ctx.run();

        if (importTimedOut || compileTimedOut)
        {
            /// Reap any processes that were terminated.
            boost::system::error_code errCode;
            if (importProc.running(errCode))
            {
                importProc.wait(errCode);
            }
            if (compileProc.running(errCode))
            {
                compileProc.wait(errCode);
            }
            if (importTimedOut)
            {
                return std::unexpected(ModelLoadError("Model import timed out after 10 seconds"));
            }
            return std::unexpected(ModelLoadError("Model compile timed out after 20 seconds"));
        }

        const bool success = (importExitCode == 0) && (compileExitCode == 0);
        if (success)
        {
            /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
            auto modelVmfb1 = std::make_shared<std::byte[]>(modelVmfb.size());
            auto modelVmfb1Span = std::span{modelVmfb1.get(), modelVmfb.size()};
            std::ranges::copy(modelVmfb, modelVmfb1Span.begin());

            ModelMetadataGraph modelGraph(graphPath);
            auto metadata = modelGraph.getModelMetadata();

            Model model = Model{std::move(modelVmfb1), modelVmfb.size()};
            model.functionName = "module." + metadata.functionName;
            model.shape = metadata.inputShape;
            model.dims = metadata.inputShape.size();
            model.inputSizeInBytes
                = sizeof(float) * std::accumulate(model.shape.begin(), model.shape.end(), size_t{1}, std::multiplies<>());

            model.outputShape = metadata.outputShape;
            model.outputDims = metadata.outputShape.size();
            model.outputSizeInBytes
                = sizeof(float) * std::accumulate(model.outputShape.begin(), model.outputShape.end(), size_t{1}, std::multiplies<>());

            return model;
        }

        /// Read stderr from both processes for error reporting.
        std::string importStderr = readAllFromPipe(importStderrPipe);
        std::string compileStderr = readAllFromPipe(compileStderrPipe);

        NES_ERROR(
            "Errors during Model Import:\nIree Import Error:\n{} {}\n```\n{}```\nIree Compile Error:\n{} {}\n```\n{}```",
            bpv2::environment::find_executable("iree-import-onnx").string(),
            fmt::join(importArgs, " "),
            importStderr,
            bpv2::environment::find_executable("iree-compile").string(),
            fmt::join(compileArgs, " "),
            compileStderr);
        return std::unexpected(ModelLoadError("Model Import was not successful: Non Zero Exit Code."));
    }
    catch (boost::system::system_error& e)
    {
        return std::unexpected(ModelLoadError(fmt::format("Model Import was not successful: {}", e.what())));
    }
}
}
