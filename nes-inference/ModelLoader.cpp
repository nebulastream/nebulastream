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

#define BOOST_PROCESS_VERSION 1

#include <ModelLoader.hpp>

#include <Model.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <functional>
#include <future>
#include <ios>
#include <iostream>
#include <iterator>
#include <numeric>
#include <optional>
#include <ranges>
#include <regex>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/process/v1/child.hpp>
#include <boost/process/v1/io.hpp>
#include <boost/process/v1/pipe.hpp>
#include <boost/process/v1/search_path.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <scope_guard.hpp>

namespace bp = boost::process;
using namespace std::literals;

namespace NES::Inference
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

    typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::directedS, VertexProps> Graph;
    Graph graph;

    ModelMetadataGraph(const std::string& dot_file_path)
    {
        boost::dynamic_properties dp = boost::dynamic_properties(boost::ignore_other_properties);
        dp.property("label", boost::get(&VertexProps::label, graph));
        dp.property("shape", boost::get(&VertexProps::shape, graph));

        std::ifstream in(dot_file_path);
        boost::read_graphviz(in, graph, dp);
    }

    std::vector<size_t> parseTensorShape(const std::string& label)
    {
        /// Match tensor shapes with any element type (e.g., f32, f64, i32)
        const std::regex tensorRegex(R"(tensor<([?0-9x]+)([a-z][a-z0-9]+)>)");
        std::smatch match;
        std::vector<size_t> result;
        if (std::regex_search(label, match, tensorRegex))
        {
            std::string elementType = match[2];
            if (elementType != "f32")
            {
                NES_ERROR("Unsupported tensor element type '{}' in label '{}'. Only f32 is supported.", elementType, label);
                return {};
            }
            std::string shape_str = match[1];
            std::stringstream ss(shape_str);
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

    std::string parseFunctionName(const std::string& label)
    {
        std::regex graph_name_regex(R"(@([a-zA-Z0-9_]+)\$)");
        std::smatch match;
        if (std::regex_search(label, match, graph_name_regex))
        {
            return match[1];
        }
        return {};
    }

    /// Function to get next node label if there's exactly one outgoing edge
    std::optional<std::string> findLabelOfUnambiguousSuccessor(auto v, const auto& g)
    {
        auto [ai, ai_end] = adjacent_vertices(v, g);
        if (ai == ai_end)
        {
            return std::nullopt;
        }

        auto next_vertex = *ai;
        ++ai;
        if (ai != ai_end)
        {
            return std::nullopt;
        }

        return g[next_vertex].label;
    }

    ModelMetadata getModelMetadata()
    {
        ModelMetadata metadata;
        for (auto v : boost::make_iterator_range(boost::vertices(graph)))
        {
            const std::string& label = graph[v].label;

            if (label.find("hal.tensor.import") != std::string::npos)
            {
                metadata.inputShape = parseTensorShape(label);
            }
            else if (label.find("flow.dispatch") != std::string::npos)
            {
                metadata.functionName = parseFunctionName(label);
            }
            else if (findLabelOfUnambiguousSuccessor(v, graph)
                         .transform([](const auto& label) { return label.find("hal.tensor.export") != std::string::npos; })
                         .value_or(false))
            {
                metadata.outputShape = parseTensorShape(label);
            }
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
                bp::ipstream pipe_stream;
                bp::child c(binaryInPath, "--version", bp::std_out > pipe_stream);

                std::string line;
                while (pipe_stream && std::getline(pipe_stream, line))
                {
                    tool.version += line + "\n";
                }

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
#ifndef NES_INFERENCE_SUPPORT
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
    std::string graphPath = graphDir.string() + "/model.dot";
    compileArgs.emplace_back("--iree-flow-dump-dispatch-graph");
    compileArgs.emplace_back("--iree-flow-dump-dispatch-graph-output-file=" + graphPath);

    try
    {
        bp::pipe mlir_pipe;
        bp::ipstream import_error;
        bp::ipstream compile_error;
        bp::ipstream model_stream;
        std::vector<bp::child> process;
        process.emplace_back(bp::search_path("iree-import-onnx"), importArgs, bp::std_out > mlir_pipe, bp::std_err > import_error);
        process.emplace_back(
            bp::search_path("iree-compile"), compileArgs, bp::std_in<mlir_pipe, bp::std_out> model_stream, bp::std_err > compile_error);

        std::vector<std::byte> modelVmfb;
        std::array<std::byte, 4096> buffer;

        while (model_stream.read(reinterpret_cast<char*>(buffer.data()), buffer.size()))
        {
            std::ranges::copy(std::span{buffer.data(), static_cast<size_t>(model_stream.gcount())}, std::back_inserter(modelVmfb));
        }
        /// read() returns false on EOF or error; gcount() still holds the bytes from the final partial read
        if (model_stream.gcount() > 0)
        {
            std::ranges::copy(std::span{buffer.data(), static_cast<size_t>(model_stream.gcount())}, std::back_inserter(modelVmfb));
        }

        /// boost::process v1 wait_for() uses sigtimedwait on POSIX, which can miss
        /// SIGCHLD and block for the full timeout even after the process exits
        /// (boostorg/process#69). We work around this by calling wait() in a background
        /// thread and using std::future::wait_for for the timeout.
        auto waitFuture = std::async(std::launch::async, [&]() { std::ranges::for_each(process, [](auto& proc) { proc.wait(); }); });
        if (waitFuture.wait_for(std::chrono::seconds(10)) == std::future_status::timeout)
        {
            for (auto& proc : process)
            {
                proc.terminate();
            }
            return std::unexpected(ModelLoadError("Model import/compile timed out after 10 seconds"));
        }
        const auto success = std::ranges::all_of(process, [](const auto& process) { return process.exit_code() == 0; });
        if (success)
        {
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
        NES_ERROR(
            "Errors during Model Import:\nIree Import Error:\n{} {}\n```\n{}```\nIree Compile Error:\n{} {}\n```\n{}```",
            bp::search_path("iree-import-onnx").string(),
            fmt::join(importArgs, " "),
            std::string{std::istreambuf_iterator(import_error), std::istreambuf_iterator<char>()},
            bp::search_path("iree-compile").string(),
            fmt::join(compileArgs, " "),
            std::string{std::istreambuf_iterator(compile_error), std::istreambuf_iterator<char>()});
        return std::unexpected(ModelLoadError("Model Import was not successful: Non Zero Exit Code."));
    }
    catch (bp::process_error& bpe)
    {
        return std::unexpected(ModelLoadError(fmt::format("Model Import was not successful: {}", bpe.what())));
    }
}
}
