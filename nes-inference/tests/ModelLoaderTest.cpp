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

#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    #include <IREE/IreeCompiler.hpp>
    #include <IREE/IreeImporter.hpp>
#endif
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
    #include <OpenVINO/OpenVinoImporter.hpp>
#endif
#include <gtest/gtest.h>
#include <Model.hpp>

namespace NES
{

namespace
{
bool ireeBackendBuilt()
{
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    return true;
#else
    return false;
#endif
}

bool inferenceEnabled()
{
#ifdef NES_ENABLE_INFERENCE_BACKEND_IREE
    static const IreeImporter Importer;
    static const IreeCompiler Compiler;
    return Importer.available() && Compiler.available();
#else
    return false;
#endif
}

bool openVinoEnabled()
{
#ifdef NES_ENABLE_INFERENCE_BACKEND_OPENVINO
    static const OpenVinoImporter Importer;
    return Importer.available();
#else
    return false;
#endif
}
}

TEST(ModelLoaderTest, checkToolAvailability)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    ASSERT_TRUE(inferenceEnabled()) << "IREE inference tools not available — test binary should not be built without them";
}

namespace
{

/// End-to-end helper: import + compile. The IREE importer also runs the MLIR
/// analyzer internally so the returned `ImportedModel` is fully populated.
std::expected<CompiledModel, std::string> importAndCompile(const std::filesystem::path& path, ModelBackend backend)
{
    auto imported = importModel(path, backend);
    if (!imported)
    {
        return std::unexpected(imported.error().message);
    }
    auto compiled = compileModel(*imported);
    if (!compiled)
    {
        return std::unexpected(compiled.error().message);
    }
    return std::move(*compiled);
}

std::expected<CompiledModel, std::string> importAndCompile(const std::filesystem::path& path)
{
    return importAndCompile(path, ModelBackend::IREE);
}

std::expected<CompiledModel, std::string> importAndCompileDefault(const std::filesystem::path& path)
{
    auto imported = importModel(path);
    if (!imported)
    {
        return std::unexpected(imported.error().message);
    }
    auto compiled = compileModel(*imported);
    if (!compiled)
    {
        return std::unexpected(compiled.error().message);
    }
    return std::move(*compiled);
}

}

TEST(ModelLoaderTest, LoadsIdentityModel)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto result = importAndCompile(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load identity model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsIdentityModelWithOpenVinoBackend)
{
    if (!openVinoEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto imported = importModel(path, ModelBackend::OPENVINO);
    ASSERT_TRUE(imported.has_value()) << "Failed to import OpenVINO model: " << imported.error().message;

    EXPECT_EQ(imported->getBackend(), ModelBackend::OPENVINO);
    EXPECT_EQ(imported->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(imported->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_FALSE(imported->empty());

    auto compiled = compileModel(*imported);
    ASSERT_TRUE(compiled.has_value()) << "Failed to compile OpenVINO model: " << (compiled ? "" : compiled.error().message);
    EXPECT_EQ(compiled->getBackend(), ModelBackend::OPENVINO);
    EXPECT_EQ(compiled->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(compiled->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_FALSE(compiled->empty());
}

TEST(ModelLoaderTest, LoadsIdentityModelWithDefaultOpenVinoBackend)
{
    if (!openVinoEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto result = importAndCompileDefault(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load identity model with default backend: " << (result ? "" : result.error());
    EXPECT_EQ(result->getBackend(), ModelBackend::OPENVINO);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsReductionModel)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_reduction.onnx";
    auto result = importAndCompile(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load reduction model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 40U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsReductionModelWithOpenVinoBackend)
{
    if (!openVinoEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_reduction.onnx";
    auto result = importAndCompile(path, ModelBackend::OPENVINO);
    ASSERT_TRUE(result.has_value()) << "Failed to load OpenVINO reduction model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getBackend(), ModelBackend::OPENVINO);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 40U);
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsExpansionModel)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_expansion.onnx";
    auto result = importAndCompile(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load expansion model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 40U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsExpansionModelWithOpenVinoBackend)
{
    if (!openVinoEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_expansion.onnx";
    auto result = importAndCompile(path, ModelBackend::OPENVINO);
    ASSERT_TRUE(result.has_value()) << "Failed to load OpenVINO expansion model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getBackend(), ModelBackend::OPENVINO);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 40U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadInvalidPath)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    auto imported = importModel("nonexistent.onnx", ModelBackend::IREE);
    EXPECT_FALSE(imported.has_value());
}

TEST(ModelLoaderTest, LoadNonOnnxExtensionIsRejected)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    auto imported = importModel("model.pt", ModelBackend::IREE);
    EXPECT_FALSE(imported.has_value());
    EXPECT_NE(imported.error().message.find(".onnx"), std::string::npos);
}

TEST(ModelLoaderTest, LoadUnsupportedOpenVinoExtensionIsRejected)
{
    if (!openVinoEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto imported = importModel("model.pt", ModelBackend::OPENVINO);
    EXPECT_FALSE(imported.has_value());
    EXPECT_NE(imported.error().message.find(".pt2"), std::string::npos);
}

TEST(ModelLoaderTest, LoadCorruptOnnxFile)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    auto corruptFile = std::filesystem::temp_directory_path() / "corrupt_model.onnx";
    {
        std::ofstream out(corruptFile, std::ios::binary);
        out << "this is not a valid onnx model";
    }

    auto imported = importModel(corruptFile, ModelBackend::IREE);
    EXPECT_FALSE(imported.has_value());
    std::filesystem::remove(corruptFile);
}

TEST(ModelLoaderTest, LoadEmptyOnnxFile)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    auto emptyFile = std::filesystem::temp_directory_path() / "empty_model.onnx";
    {
        const std::ofstream out(emptyFile, std::ios::binary);
    }

    auto imported = importModel(emptyFile, ModelBackend::IREE);
    EXPECT_FALSE(imported.has_value());
    std::filesystem::remove(emptyFile);
}

TEST(ModelLoaderTest, TempDirectoryIsCleanedUpOnSuccess)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto imported = importModel(path, ModelBackend::IREE);
    ASSERT_TRUE(imported.has_value());

    /// Verify no nes-graph-<our-pid>-* directories remain
    auto pid = std::to_string(getpid());
    for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
    {
        auto name = entry.path().filename().string();
        EXPECT_FALSE(name.starts_with("nes-graph-" + pid)) << "Leftover temp directory: " << name;
    }
}

TEST(ModelLoaderTest, TempDirectoryIsCleanedUpOnFailure)
{
    if (!ireeBackendBuilt())
    {
        GTEST_SKIP() << "IREE backend was not built";
    }
    auto corruptFile = std::filesystem::temp_directory_path() / "corrupt_for_cleanup.onnx";
    {
        std::ofstream out(corruptFile, std::ios::binary);
        out << "not valid onnx data";
    }

    auto imported = importModel(corruptFile, ModelBackend::IREE);
    EXPECT_FALSE(imported.has_value());

    auto pid = std::to_string(getpid());
    for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
    {
        auto name = entry.path().filename().string();
        EXPECT_FALSE(name.starts_with("nes-graph-" + pid)) << "Leftover temp directory: " << name;
    }
    std::filesystem::remove(corruptFile);
}

}
