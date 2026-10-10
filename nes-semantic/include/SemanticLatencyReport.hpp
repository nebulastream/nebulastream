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

#include <string_view>

namespace NES
{

/// Writes one summary line for the model round trips that `HttpLatencyStats` measured, then starts
/// the measurement over, so a log carries one line per query rather than one per process. `context`
/// names the execution mode, "synchronous" or "asynchronous".
///
/// The measurement itself lives in nes-http, where the calls are made; only the wording lives here.
/// That split is not cosmetic: scripts/run_cluster_benchmark.sh and
/// docs/design/20261006_async_operator_measurements.md grep for this exact phrasing and set
/// NES_SEMANTIC_LATENCY_REPORT, so the line has to keep saying what it has always said even though
/// nothing below this function is about semantic models any more.
void reportAndResetSemanticLatency(std::string_view context);

}
