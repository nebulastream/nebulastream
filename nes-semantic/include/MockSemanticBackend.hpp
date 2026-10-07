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

#include <chrono>
#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <SemanticBackend.hpp>

namespace NES
{

/// Deterministic, network-free backend for system and unit tests, selected with
/// `'mock' AS LLM.BACKEND`. `LLM.ENDPOINT` picks the behaviour:
///
///   echo          answers every row with its input text upper-cased
///   label:<X>     answers every row with X
///   unparseable   answers with prose instead of JSON
///   fail          fails like an unreachable endpoint
///
/// Any of them may be followed by `@<ms>`, as in `echo@200`, which makes one request take that
/// long. Waiting is what a model actually does, and nothing else in a hermetic test can stand in
/// for it: it is how the asynchronous framework's throughput is measured, and how a blocking call
/// occupying a worker thread is demonstrated, without a model server.
///
/// It returns raw response text, exactly as the HTTP backend would after unwrapping the
/// chat-completion envelope, so the real `SemanticMapCodec` decodes it: the system tests exercise
/// the actual parse cascade and answer normalization. For the same reason the answer envelope uses
/// lower-cased output field names, the way real models tend to write them.
///
/// `outputColumns` are the columns the prompt asks for, filter verdicts (`__filter_<k>`) included;
/// every one gets the same answer. Without columns the mock answers in the flat
/// `{row_id: {answer, confidence}}` envelope a filter-only prompt requests. Filter verdicts are
/// therefore steered through the data or the label: `echo` passes a row whose text is "true" or
/// "yes", and `label:true` / `label:false` pass or drop every row.
class MockSemanticBackend final : public SemanticBackend
{
public:
    enum class Mode : uint8_t
    {
        ECHO,
        LABEL,
        UNPARSEABLE,
        FAIL,
    };

    /// Throws InvalidSemanticModel for an unknown behaviour.
    MockSemanticBackend(std::string_view behaviour, std::vector<std::string> outputColumns);

    /// Validation hook for the catalog, so a typo fails CREATE rather than the first record.
    [[nodiscard]] static bool isValidBehaviour(std::string_view behaviour);

    [[nodiscard]] std::expected<std::string, BackendError> complete(const CompletionRequest& request) override;

private:
    Mode mode;
    std::string label;
    std::vector<std::string> outputColumns;
    std::chrono::milliseconds delay{0};
};

}
