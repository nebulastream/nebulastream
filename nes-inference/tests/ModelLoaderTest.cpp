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
    auto result = NES::Nebuli::Inference::load(
        std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx", NES::Nebuli::Inference::ModelOptions{.opset = 17});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->getByteCode().size(), 29291384)
        << "The Size of the Model does not match. Reproduce by using: \n"
           "iree-import-onnx convit_tiny_Opset17.onnx --opset-version 17 | \\ \n"
           "iree-compile - --iree-hal-target-device=local "
           "--iree-hal-local-target-device-backends=llvm-cpu --iree-llvmcpu-target-cpu=host | \\ \n"
           "wc -c";
}

TEST(Model, TestModelSerialization)
{
    auto result = NES::Nebuli::Inference::load(
        std::filesystem::path(TEST_DATA_DIR) / "convit_tiny_Opset17.onnx", NES::Nebuli::Inference::ModelOptions{.opset = 17});
    ASSERT_TRUE(result.has_value());

    auto serialized = NES::SerializableOperator_Model();
    NES::Nebuli::Inference::serializeModel(*result, serialized);
    auto deserialized = NES::Nebuli::Inference::deserializeModel(serialized);
    ASSERT_EQ(*result, deserialized);
}
