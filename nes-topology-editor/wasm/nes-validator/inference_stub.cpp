// Stub implementation of nes-inference's IREE-backed importer/compiler for the
// WASM build. The real implementations in nes-inference/src/Inference.cpp pull
// in IREE/MLIR, which cannot run in the browser. The validator only needs the
// InferModelResolutionRule type to exist so SemanticAnalyzer compiles; it
// never executes a model. Returning an error from importModel/compileModel is
// safe — without registered models, the optimizer rule has no work to do.
#include <expected>
#include <filesystem>

#include <Inference.hpp>
#include <Model.hpp>

namespace NES
{

std::expected<ImportedModel, ImportError> importModel(const std::filesystem::path&)
{
    return std::unexpected(ImportError{"Model inference is not supported in the web validator"});
}

std::expected<CompiledModel, CompileError> compileModel(const ImportedModel&)
{
    return std::unexpected(CompileError{"Model inference is not supported in the web validator"});
}

}
