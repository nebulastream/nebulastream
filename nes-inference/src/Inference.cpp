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

#include <Inference.hpp>

#include <expected>
#include <filesystem>
#include <utility>

#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    #include <IREE/IreeCompiler.hpp>
    #include <IREE/IreeImporter.hpp>
#endif
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
    #include <OpenVINO/OpenVinoImporter.hpp>
#endif
#include <Model.hpp>
#include <ModelAccess.hpp>

namespace NES
{

namespace
{

/// Each class's ctor forks a `--version` subprocess for tool discovery, so
/// amortize across calls by keeping one instance per process.
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
const IreeImporter& sharedImporter()
{
    static const IreeImporter Instance;
    return Instance;
}

const IreeCompiler& sharedCompiler()
{
    static const IreeCompiler Instance;
    return Instance;
}
#endif

#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
const OpenVinoImporter& sharedOpenVinoImporter()
{
    static const OpenVinoImporter Instance;
    return Instance;
}
#endif

}

ModelBackend defaultModelBackend()
{
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
    return ModelBackend::OPENVINO;
#elifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    return ModelBackend::IREE;
#else
    #error "At least one inference backend must be enabled"
#endif
}

std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path& modelPath)
{
    return importModel(modelPath, defaultModelBackend());
}

std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path& modelPath, ModelBackend backend)
{
    switch (backend)
    {
        case ModelBackend::IREE:
#if defined(NES_ENABLE_INFERENCE_BACKEND_IREE)
            return sharedImporter().importOnnx(modelPath);
#else
            return std::unexpected(ImportError{"IREE inference backend was not built"});
#endif
        case ModelBackend::OPENVINO:
#if defined(NES_ENABLE_INFERENCE_BACKEND_OPENVINO)
            return sharedOpenVinoImporter().importModel(modelPath);
#else
            return std::unexpected(ImportError{"OpenVINO inference backend was not built"});
#endif
    }
    std::unreachable();
}

std::expected<CompiledModel, CompileError> compileModel(const ImportedModel& imported)
{
    switch (imported.getBackend())
    {
        case ModelBackend::IREE:
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
            return sharedCompiler().compile(imported);
#else
            return std::unexpected(CompileError{"IREE inference backend was not built"});
#endif
        case ModelBackend::OPENVINO:
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
            return detail::ModelAccess::compileFrom(imported, detail::RefCountedByteBuffer::fromBytes(imported.getData()));
#else
            return std::unexpected(CompileError{"OpenVINO inference backend was not built"});
#endif
    }
    std::unreachable();
}

}
