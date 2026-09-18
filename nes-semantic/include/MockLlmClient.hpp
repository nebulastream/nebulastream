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

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <LlmClient.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{

/// Hermetic stand-in for `CurlLlmClient` (plan §M4): answers without touching a socket, so the
/// systest can exercise grammar, binding, resolution, lowering and JIT execution while the only
/// fake thing is the HTTP layer. Selected by the factory for `mock://` base URLs; the behaviour
/// is the URL authority, `mock://<behaviour>` (the rest of the URL is reserved, currently unused).
///
/// The `echo` behaviour deliberately routes its raw answer through the same `normalizeAnswer`
/// the curl client uses, so a systest against the mock is a test of the §2.3 normalisation
/// cascade, not of the mock.
class MockLlmClient final : public LlmClient
{
public:
    /// Same constructor shape as `CurlLlmClient` — the factory treats the two identically.
    /// `outputFieldNames` are the declared OUTPUT field base names (e.g. "sentiment").
    MockLlmClient(SemanticModelConfig config, std::vector<std::string> outputFieldNames);

    SemanticMapResult map(std::string_view inputText) override;

private:
    SemanticModelConfig config;
    std::vector<std::string> outputFieldNames;
};

}