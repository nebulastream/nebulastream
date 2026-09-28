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

#include <InferenceRuntime.hpp>

#include <cstddef>
#include <cstring>
#include <expected>
#include <filesystem>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <OpenVINO/OpenVinoImporter.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <Inference.hpp>
#include <Model.hpp>
#include <OpenVinoRuntimeBackend.hpp>

namespace NES
{

namespace
{

bool inferenceEnabled()
{
    static const OpenVinoImporter Importer;
    return Importer.available();
}

std::expected<CompiledModel, std::string> load(const std::string& name)
{
    auto imported = importModel(std::filesystem::path(INFERENCE_TEST_DATA) / name);
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

std::vector<float> runInference(const CompiledModel& model, const std::vector<float>& input)
{
    InferenceRuntime runtime;
    runtime.setup(model);
    std::memcpy(runtime.getInputData(), input.data(), input.size() * sizeof(float));
    runtime.infer();

    std::vector<float> output(runtime.getOutputSize() / sizeof(float));
    std::memcpy(output.data(), runtime.getOutputData(), runtime.getOutputSize());
    return output;
}

std::vector<float> ascending(size_t count)
{
    std::vector<float> values(count);
    /// NOLINTNEXTLINE(modernize-use-ranges) std::ranges::iota not yet available in libc++
    std::iota(values.begin(), values.end(), 1.0F);
    return values;
}

/// Runs `infer` on a fresh backend with caller-sized buffers and returns the failure message,
/// or an empty string if inference did not throw InferenceRuntimeFailure.
std::string inferFailureMessage(const CompiledModel& model, size_t inputBufferSize, size_t outputBufferSize)
{
    OpenVinoRuntimeBackend backend;
    backend.setup(model);
    std::vector<std::byte> input(inputBufferSize);
    std::vector<std::byte> output(outputBufferSize);
    try
    {
        backend.infer(input.data(), input.size(), output.data(), output.size());
    }
    catch (const Exception& ex)
    {
        EXPECT_EQ(ex.code(), ErrorCode::InferenceRuntimeFailure);
        return ex.what();
    }
    return {};
}

}

/// `setup` caches compiled models process-wide so worker threads sharing one model do not
/// each pay the OpenVINO compile. Two runtimes over the same model must still infer
/// independently and correctly — the second one takes the cached path.
TEST(InferenceRuntimeTest, RepeatedSetupOfSameModelStaysCorrect)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto identity = load("tiny_identity.onnx");
    ASSERT_TRUE(identity.has_value()) << identity.error();

    const auto input = ascending(100);
    const auto first = runInference(*identity, input);
    const auto second = runInference(*identity, input);

    ASSERT_EQ(first.size(), input.size());
    EXPECT_EQ(first, input);
    EXPECT_EQ(second, first);
}

/// Guards the cache key: two different models must not resolve to the same compiled model.
TEST(InferenceRuntimeTest, DifferentModelsDoNotShareACacheEntry)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto identity = load("tiny_identity.onnx");
    ASSERT_TRUE(identity.has_value()) << identity.error();
    auto reduction = load("tiny_reduction.onnx");
    ASSERT_TRUE(reduction.has_value()) << reduction.error();

    const auto input = ascending(100);
    const auto identityOutput = runInference(*identity, input);
    const auto reductionOutput = runInference(*reduction, input);

    EXPECT_EQ(identityOutput.size(), 100U);
    EXPECT_EQ(reductionOutput.size(), 10U);
    EXPECT_EQ(identityOutput, input);

    /// And back again: the identity model must not have been displaced by the reduction one.
    EXPECT_EQ(runInference(*identity, input), input);
}

/// The input buffer holds all input tensors back to back, in the model's declaration order.
/// tiny_two_inputs.onnx concatenates a [1,2] and b [1,3], so the output mirrors the input
/// buffer exactly when every tensor is bound to the right slice.
TEST(InferenceRuntimeTest, InfersModelWithTwoInputs)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto twoInputs = load("tiny_two_inputs.onnx");
    ASSERT_TRUE(twoInputs.has_value()) << twoInputs.error();

    InferenceRuntime runtime;
    runtime.setup(*twoInputs);
    ASSERT_EQ(runtime.getInputSize(), 5 * sizeof(float));

    const std::vector<float> input{1.0F, 2.0F, 10.0F, 20.0F, 30.0F};
    EXPECT_EQ(runInference(*twoInputs, input), input);
}

/// tiny_clip_infinite_max.onnx computes Clip(x, 0, +inf), which `ovc` writes as a Clamp with max="inf".
/// OpenVINO's IR reader parses "inf" as 0, so without the importer's rewrite every output would be 0.
TEST(InferenceRuntimeTest, InfiniteClampBoundSurvivesImport)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto clip = load("tiny_clip_infinite_max.onnx");
    ASSERT_TRUE(clip.has_value()) << clip.error();

    const std::vector<float> input{-1.0F, 0.5F, 2.0F, 1e30F};
    const std::vector<float> expected{0.0F, 0.5F, 2.0F, 1e30F};
    EXPECT_EQ(runInference(*clip, input), expected);
}

/// When the buffer runs out part-way through, the error names the input tensor that did not fit.
TEST(InferenceRuntimeTest, InputBufferTooSmallNamesTheTensor)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto twoInputs = load("tiny_two_inputs.onnx");
    ASSERT_TRUE(twoInputs.has_value()) << twoInputs.error();

    /// Room for tensor 0 (8 B) and half of tensor 1 (12 B).
    const auto secondMissing = inferFailureMessage(*twoInputs, 14, twoInputs->outputSize());
    EXPECT_NE(secondMissing.find("Input tensor 1 needs 12 B, but only 6 B"), std::string::npos) << secondMissing;

    /// Not even tensor 0 fits.
    const auto firstMissing = inferFailureMessage(*twoInputs, 4, twoInputs->outputSize());
    EXPECT_NE(firstMissing.find("Input tensor 0 needs 8 B, but only 4 B"), std::string::npos) << firstMissing;
}

TEST(InferenceRuntimeTest, OutputBufferTooSmallIsRejected)
{
    if (!inferenceEnabled())
    {
        GTEST_SKIP() << "OpenVINO import unavailable in this environment";
    }

    auto twoInputs = load("tiny_two_inputs.onnx");
    ASSERT_TRUE(twoInputs.has_value()) << twoInputs.error();

    const auto message = inferFailureMessage(*twoInputs, twoInputs->inputSize(), twoInputs->outputSize() - 1);
    EXPECT_NE(message.find("insufficient for model output size 20 B"), std::string::npos) << message;
}

}
