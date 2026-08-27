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
#include <Inference.hpp>
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

}
