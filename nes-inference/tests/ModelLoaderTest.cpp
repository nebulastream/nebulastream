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

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <unistd.h>

#include <ModelLoader.hpp>

namespace NES::Inference
{

TEST(ModelLoaderTest, checkToolAvailability)
{
    ASSERT_TRUE(enabled()) << "IREE inference tools not available — test binary should not be built without them";
}

TEST(ModelLoaderTest, LoadsIdentityModel)
{
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto result = load(path, {});
    ASSERT_TRUE(result.has_value()) << "Failed to load identity model: " << (result ? "" : result.error().message);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->getByteCode().empty());
}

TEST(ModelLoaderTest, LoadsReductionModel)
{
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_reduction.onnx";
    auto result = load(path, {});
    ASSERT_TRUE(result.has_value()) << "Failed to load reduction model: " << (result ? "" : result.error().message);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->inputSize(), 400U);
    EXPECT_EQ(result->outputSize(), 40U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->getByteCode().empty());
}

TEST(ModelLoaderTest, LoadsExpansionModel)
{
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_expansion.onnx";
    auto result = load(path, {});
    ASSERT_TRUE(result.has_value()) << "Failed to load expansion model: " << (result ? "" : result.error().message);
    EXPECT_EQ(result->getInputShape(), (std::vector<size_t>{1, 10}));
    EXPECT_EQ(result->getOutputShape(), (std::vector<size_t>{1, 100}));
    EXPECT_EQ(result->inputSize(), 40U);
    EXPECT_EQ(result->outputSize(), 400U);
    EXPECT_FALSE(result->getFunctionName().empty());
    EXPECT_FALSE(result->getByteCode().empty());
}

TEST(ModelLoaderTest, LoadInvalidPath)
{
    auto result = load("nonexistent.onnx", {});
    EXPECT_FALSE(result.has_value());
}

TEST(ModelLoaderTest, LoadNonOnnxExtensionIsRejected)
{
    auto result = load("model.pt", {});
    EXPECT_FALSE(result.has_value());
    EXPECT_NE(result.error().message.find(".onnx"), std::string::npos);
}

TEST(ModelLoaderTest, LoadCorruptOnnxFile)
{
    auto corruptFile = std::filesystem::temp_directory_path() / "corrupt_model.onnx";
    {
        std::ofstream out(corruptFile, std::ios::binary);
        out << "this is not a valid onnx model";
    }

    auto result = load(corruptFile, {});
    EXPECT_FALSE(result.has_value());
    std::filesystem::remove(corruptFile);
}

TEST(ModelLoaderTest, LoadEmptyOnnxFile)
{
    auto emptyFile = std::filesystem::temp_directory_path() / "empty_model.onnx";
    {
        const std::ofstream out(emptyFile, std::ios::binary);
    }

    auto result = load(emptyFile, {});
    EXPECT_FALSE(result.has_value());
    std::filesystem::remove(emptyFile);
}

TEST(ModelLoaderTest, TempDirectoryIsCleanedUpOnSuccess)
{
    const std::string path = std::string(INFERENCE_TEST_DATA) + "/tiny_identity.onnx";
    auto result = load(path, {});
    ASSERT_TRUE(result.has_value());

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
    auto corruptFile = std::filesystem::temp_directory_path() / "corrupt_for_cleanup.onnx";
    {
        std::ofstream out(corruptFile, std::ios::binary);
        out << "not valid onnx data";
    }

    auto result = load(corruptFile, {});
    EXPECT_FALSE(result.has_value());

    auto pid = std::to_string(getpid());
    for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::temp_directory_path()))
    {
        auto name = entry.path().filename().string();
        EXPECT_FALSE(name.starts_with("nes-graph-" + pid)) << "Leftover temp directory: " << name;
    }
    std::filesystem::remove(corruptFile);
}

}
