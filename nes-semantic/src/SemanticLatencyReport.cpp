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

#include <SemanticLatencyReport.hpp>

#include <cstdio>
#include <cstdlib>
#include <string_view>

#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

#include <Http/HttpLatencyStats.hpp>

namespace NES
{

void reportAndResetSemanticLatency(const std::string_view context)
{
    if (const auto summary = HttpLatencyStats::instance().summarize())
    {
        /// A release build compiles with NES_LOGLEVEL_WARN, which removes NES_INFO entirely -- and
        /// a release build is exactly where a benchmark runs. This is a measurement, not a
        /// diagnostic, so it must not depend on how the logger was compiled. Opt in with
        /// NES_SEMANTIC_LATENCY_REPORT=1 and it goes to stderr as well, which keeps an ordinary
        /// deployment quiet.
        if (std::getenv("NES_SEMANTIC_LATENCY_REPORT") != nullptr)
        {
            fmt::print(
                stderr,
                "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, max {:.0f} ms, "
                "{:.1f} calls/s\n",
                context,
                summary->calls,
                summary->failures,
                summary->meanMs,
                summary->p50Ms,
                summary->p95Ms,
                summary->maxMs,
                summary->callsPerSecond);
        }
        NES_INFO(
            "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, max {:.0f} ms, "
            "{:.1f} calls/s",
            context,
            summary->calls,
            summary->failures,
            summary->meanMs,
            summary->p50Ms,
            summary->p95Ms,
            summary->maxMs,
            summary->callsPerSecond);
    }
    HttpLatencyStats::instance().reset();
}

}
