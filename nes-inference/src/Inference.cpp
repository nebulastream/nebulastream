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

#include <OpenVINO/OpenVinoImporter.hpp>
#include <Model.hpp>
#include <ModelAccess.hpp>

namespace NES
{

namespace
{

/// The importer's ctor forks a `--version` subprocess for tool discovery, so
/// amortize across calls by keeping one instance per process.
const OpenVinoImporter& sharedImporter()
{
    static const OpenVinoImporter Instance;
    return Instance;
}

}

std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path& modelPath)
{
    return sharedImporter().importModel(modelPath);
}

std::expected<CompiledModel, CompileError> compileModel(const ImportedModel& imported)
{
    /// OpenVINO consumes the IR produced at import time as-is; there is no separate
    /// ahead-of-time compilation step, so the payload just moves to the compiled stage.
    return detail::ModelAccess::compileFrom(imported, detail::RefCountedByteBuffer::fromBytes(imported.getData()));
}

}
