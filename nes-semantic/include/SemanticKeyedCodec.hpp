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

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// SEM_SCAN's prompts and response decoding, after `SEMScanOperator` of the Python reference (`operators/sem_scan.py`):
///
///     <init or fold sysprompt>
///     Instruction: <INIT_PROMPT for a key's first row, PROMPT afterwards>
///     [Dataset context: ...]
///     [Prior state for this key: "<state>"]      fold only
///     Data: {"row1": "<the row's INPUT values, space-joined>"}
class SemanticScanCodec
{
public:
    explicit SemanticScanCodec(SemanticModelConfig config);

    /// `prior` is the key's state, nullopt for its first row, which needs an INIT_PROMPT.
    [[nodiscard]] std::string buildPrompt(std::string_view text, const std::optional<std::string>& prior) const;

    struct Answer
    {
        std::string state;
        std::string confidence;
    };

    /// The row's entry, or the only entry if the model mangled the id (`_extract_answer`). Nullopt when the response holds
    /// no usable entry: the row then gets the default value and the key's state stays as it was.
    [[nodiscard]] std::optional<Answer> parse(std::string_view response) const;

private:
    SemanticModelConfig config;
};

/// One semantic window, as `Window` of `operators/sem_window.py`.
struct SemanticWindow
{
    std::string id;
    std::string description;
    std::string start;
    size_t rows = 0;
    std::string lastRow;
    std::deque<std::string> recentExamples;
};

/// The windows of one key: every window ever opened, and the active ones, oldest first.
struct SemanticWindowState
{
    std::unordered_map<std::string, SemanticWindow> windows;
    std::vector<std::string> active;
    size_t opened = 0;
};

/// SEM_WINDOW's prompt and response decoding, after `SEMWindowOperator._evaluate_windows_batch`:
///
///     <window sysprompt>
///
///     Threshold: <threshold>
///     <PROMPT>
///
///     [Dataset context: ...]Windows to score:
///     {"w1": {"description": ..., "recent_examples": [...]}, ...}   or "most_recent_tuple" for the pairwise strategy
///
///     New tuple: <the row's INPUT values, space-joined>
class SemanticWindowCodec
{
public:
    explicit SemanticWindowCodec(SemanticModelConfig config);

    [[nodiscard]] std::string buildPrompt(std::string_view text, const std::vector<const SemanticWindow*>& windows) const;

    struct Score
    {
        double continuity = 0.0;
        std::string windowDescription;
        std::string newWindowDescription;
    };

    /// One score per window, in order; a window the response does not mention scores 0 and keeps its description.
    [[nodiscard]] std::vector<Score>
    parse(std::string_view response, std::string_view text, const std::vector<const SemanticWindow*>& windows) const;

    /// The row's values for the OUTPUT fields, once `scores` (one per active window, empty for a key's first row) are in:
    /// joins the best window or opens a new one, as `_process_record` does.
    [[nodiscard]] std::vector<std::string>
    assign(SemanticWindowState& state, std::string_view text, std::string_view start, const std::vector<Score>& scores) const;

private:
    SemanticWindow& open(SemanticWindowState& state, std::string_view start, std::string description) const;
    void update(SemanticWindow& window, std::string_view text, std::string description) const;

    SemanticModelConfig config;
};

/// The state of one translated SEM_SCAN or SEM_WINDOW operator, shared by the function instances of its stage: each slot
/// of the Async source has its own instance, but a key's rows may run on any slot. The async framework runs one call
/// per key at a time; the mutex only guards the maps themselves.
struct SemanticKeyedState
{
    std::mutex mutex;
    std::unordered_map<std::string, std::string> scans;
    std::unordered_map<std::string, SemanticWindowState> windows;

    /// The state of `stateId`, created on first use and gone once the last instance holding it is.
    static std::shared_ptr<SemanticKeyedState> acquire(const std::string& stateId);
};

namespace detail
{
/// Python's `text[:count]`: the first `count` code points of UTF-8 text.
std::string truncateCodePoints(std::string_view text, size_t count);
/// Python's `str(float)` for a score.
std::string pythonFloat(double value);
}

}
