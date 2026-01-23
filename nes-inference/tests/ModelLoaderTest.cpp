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

#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>

#include <Util/Logger/LogLevel.hpp>
#include <ModelLoader.hpp>

TEST(ModelLoaderTest, TestInferenceSupport)
{
    NES::Logger::setupLogging("InferenceTest.log", NES::LogLevel::LOG_DEBUG);
#ifdef NEBULI_INFERENCE_SUPPORT
    ASSERT_TRUE(NES::Nebuli::Inference::enabled());
#else
    ASSERT_FALSE(NES::Nebuli::Inference::enabled());
#endif
}

TEST(ModelLoaderTest, TestLoadingModel)
{
    NES::Logger::setupLogging("ModelLoaderTest.log", NES::LogLevel::LOG_DEBUG);
    if (!NES::Nebuli::Inference::enabled())
    {
        GTEST_SKIP() << "Not build with inference";
    }

    ASSERT_TRUE(std::filesystem::exists(std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx"));

    auto result = NES::Nebuli::Inference::load(
        std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx", NES::Nebuli::Inference::ModelOptions{.opset = 17});
    ASSERT_TRUE(result.has_value()) << "Loading model failed: " << result.error().message;
    EXPECT_GE(result->getByteCode().size(), 25000000)
        << "The estimated size of the Model does not match. Reproduce by using: \n"
           "iree-import-onnx convit_tiny_Opset17.onnx --opset-version 17 | \\ \n"
           "iree-compile - --iree-hal-target-device=local "
           "--iree-hal-local-target-device-backends=llvm-cpu --iree-llvmcpu-target-cpu=host | \\ \n"
           "wc -c";
}

TEST(Model, TestModelSerialization)
{
    if (!NES::Nebuli::Inference::enabled())
    {
        GTEST_SKIP() << "Not build with inference";
    }

    ASSERT_TRUE(std::filesystem::exists(std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx"));

    auto result = NES::Nebuli::Inference::load(
        std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx", NES::Nebuli::Inference::ModelOptions{.opset = 17});
    ASSERT_TRUE(result.has_value());

    auto serialized = NES::SerializableModel();
    NES::Nebuli::Inference::serializeModel(*result, serialized);
    auto deserialized = NES::Nebuli::Inference::deserializeModel(serialized);
    ASSERT_EQ(*result, deserialized);
}
