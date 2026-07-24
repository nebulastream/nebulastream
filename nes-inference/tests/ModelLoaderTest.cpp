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
#include <OpenVINO/OpenVinoImporter.hpp>
#include <gtest/gtest.h>
#include <Model.hpp>

namespace NES
{

namespace
{

bool inferenceEnabled()
{
    static const OpenVinoImporter Importer;
    return Importer.available();
}

/// End-to-end helper: import + compile.
std::expected<CompiledModel, std::string> importAndCompile(const std::filesystem::path& path)
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

TEST(ModelLoaderTest, checkToolAvailability)
{
    ASSERT_TRUE(inferenceEnabled()) << "ovc not available — test binary should not be built without it";
}

TEST(ModelLoaderTest, LoadsIdentityModel)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto imported = importModel(path);
    ASSERT_TRUE(imported.has_value()) << "Failed to import model: " << imported.error().message;
    EXPECT_EQ(imported->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(imported->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_FALSE(imported->empty());

    auto compiled = compileModel(*imported);
    ASSERT_TRUE(compiled.has_value()) << "Failed to compile model: " << (compiled ? "" : compiled.error().message);
    EXPECT_EQ(compiled->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(compiled->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(compiled->inputSize(), 400U);
    EXPECT_EQ(compiled->outputSize(), 400U);
    EXPECT_FALSE(compiled->empty());
}

TEST(ModelLoaderTest, LoadsReductionModel)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_reduction.onnx";
    auto result = importAndCompile(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load reduction model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 40U);
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadsExpansionModel)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_expansion.onnx";
    auto result = importAndCompile(path);
    ASSERT_TRUE(result.has_value()) << "Failed to load expansion model: " << (result ? "" : result.error());
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 40U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->empty());
}

TEST(ModelLoaderTest, LoadInvalidPath)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto imported = importModel("nonexistent.onnx");
    EXPECT_FALSE(imported.has_value());
}

TEST(ModelLoaderTest, LoadUnsupportedExtensionIsRejected)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto imported = importModel("model.pt");
    EXPECT_FALSE(imported.has_value());
    EXPECT_NE(imported.error().message.find(".pt2"), std::string::npos);
}

/// A dynamic batch dimension is the case single-tuple inference handles: resolve it to 1
/// and let the model through rather than making the user re-export it.
TEST(ModelLoaderTest, LoadModelWithDynamicBatchResolvesToOne)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_dynamic_batch.onnx";
    auto imported = importModel(path);
    ASSERT_TRUE(imported.has_value()) << imported.error().message;
    EXPECT_EQ(imported->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(imported->getOutputShape(), (std::vector<size_t>{1, 100}));
}

/// A fixed batch above 1 cannot be fed one tuple at a time.
TEST(ModelLoaderTest, LoadModelWithFixedBatchIsRejected)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_fixed_batch.onnx";
    auto imported = importModel(path);
    ASSERT_FALSE(imported.has_value());
    EXPECT_NE(imported.error().message.find("fixed batch dimension"), std::string::npos) << imported.error().message;
}

/// Only a SavedModel directory may come without an extension; a typo'd file path
/// should get the friendly format error rather than an opaque `ovc` failure.
TEST(ModelLoaderTest, LoadExtensionlessNonDirectoryIsRejected)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto imported = importModel(std::string(INFERENCE_TEST_DATA) + "/tiny_identity");
    ASSERT_FALSE(imported.has_value());
    EXPECT_NE(imported.error().message.find(".onnx"), std::string::npos) << imported.error().message;
}

TEST(ModelLoaderTest, LoadCorruptOnnxFile)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto corruptFile = std::filesystem::temp_directory_path() / "corrupt_model.onnx";
    {
        std::ofstream out(corruptFile, std::ios::binary);
        out << "this is not a valid onnx model";
    }

    auto imported = importModel(corruptFile);
    EXPECT_FALSE(imported.has_value());
    std::filesystem::remove(corruptFile);
}

TEST(ModelLoaderTest, LoadEmptyOnnxFile)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto emptyFile = std::filesystem::temp_directory_path() / "empty_model.onnx";
    {
        const std::ofstream out(emptyFile, std::ios::binary);
    }

    auto imported = importModel(emptyFile);
    EXPECT_FALSE(imported.has_value());
    std::filesystem::remove(emptyFile);
}

}
