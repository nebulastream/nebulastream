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

/// conntest-harness — connectivity-test driver (the nes-conn-test *Starter).
///
/// The harness is a long-lived stepper: it connects to the runner's
/// unix-domain control socket (`--control-sock`) and executes one command
/// per line, replying with one JSON line, until EOF. stdout/stderr stay
/// free for logs/sanitizers. See `conntest_runner/protocol.py` for the
/// reply shapes and `driver/SourceSession.hpp` / `driver/SinkSession.hpp`
/// for the command sets.
///
/// Exit codes:
///   0  --help / -h, or the session ended gracefully (EOF)
///   1  a hard control-channel error (connect/read/write failure)
///   2  bad arguments / unknown mode

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>

#include <Util/Strings.hpp>
#include <ControlChannel.hpp>
#include <SinkSession.hpp>
#include <SourceSession.hpp>

#include <argparse/argparse.hpp>

namespace NES::ConnTest
{
namespace
{

/// Register every harness flag on `program`. --mode / --config / --control-sock
/// are required; the rest are optional and, when omitted, fall back to the
/// per-mode defaults baked into SourceSessionOptions / SinkSessionOptions. The
/// runner passes them all in `--key=value` form.
void configureArgumentParser(argparse::ArgumentParser& program)
{
    program.add_argument("--mode").required().choices("source", "sink").help("which stepper to run");
    program.add_argument("--config").required().help("SQL snippet (CREATE LOGICAL/PHYSICAL SOURCE or CREATE SINK)");
    program.add_argument("--control-sock").required().help("AF_UNIX path of the runner's control channel");
    program.add_argument("--step-timeout-ms").help("per-step connector-work cap in milliseconds");
    program.add_argument("--buffer-size").help("per-buffer size in bytes");
    program.add_argument("--buffer-count").help("number of pooled buffers");
    /// Source-mode options.
    program.add_argument("--observed-path").help("write the JSONL observed per FILL here (runner decode)");
    /// Sink-mode options.
    program.add_argument("--input-path").help("formatted bytes written THROUGH the sink, sliced by WRITE <n>");
    program.add_argument("--threads").help("number of concurrent drain threads");
}

/// Override `target` from an optional unsigned `--flag`, parsed with
/// NES::from_chars. Returns false (after logging) when the flag was given but
/// is not a valid non-negative integer; leaves `target` at its default when the
/// flag is absent.
template <typename T>
bool applyUnsigned(const argparse::ArgumentParser& program, const std::string& flag, T& target)
{
    const auto raw = program.present<std::string>(flag);
    if (not raw)
    {
        return true;
    }
    const auto parsed = NES::from_chars<T>(*raw);
    if (not parsed)
    {
        std::cerr << "error: " << flag << " expects an unsigned integer, got '" << *raw << "'\n";
        return false;
    }
    target = *parsed;
    return true;
}

int runSource(const argparse::ArgumentParser& program, ControlChannel& channel)
{
    SourceSessionOptions options;
    options.configFile = program.get<std::string>("--config");

    auto stepTimeoutMs = static_cast<std::uint64_t>(options.stepTimeout.count());
    if (not applyUnsigned(program, "--buffer-size", options.bufferSize) or not applyUnsigned(program, "--buffer-count", options.bufferCount)
        or not applyUnsigned(program, "--step-timeout-ms", stepTimeoutMs))
    {
        return 2;
    }
    options.stepTimeout = std::chrono::milliseconds{stepTimeoutMs};

    if (const auto observed = program.present<std::string>("--observed-path"))
    {
        options.observedPath = std::filesystem::path{*observed};
    }
    return runSourceSession(options, channel) ? 0 : 1;
}

int runSink(const argparse::ArgumentParser& program, ControlChannel& channel)
{
    SinkSessionOptions options;
    options.configFile = program.get<std::string>("--config");

    auto stepTimeoutMs = static_cast<std::uint64_t>(options.stepTimeout.count());
    if (not applyUnsigned(program, "--buffer-size", options.bufferSize) or not applyUnsigned(program, "--buffer-count", options.bufferCount)
        or not applyUnsigned(program, "--threads", options.threads) or not applyUnsigned(program, "--step-timeout-ms", stepTimeoutMs))
    {
        return 2;
    }
    options.stepTimeout = std::chrono::milliseconds{stepTimeoutMs};

    if (const auto inputPath = program.present<std::string>("--input-path"))
    {
        options.inputPath = std::filesystem::path{*inputPath};
    }
    return runSinkSession(options, channel) ? 0 : 1;
}

}
}

/// NOLINTNEXTLINE(bugprone-exception-escape): main may propagate a truly exceptional throw; the runner SIGKILLs and respawns the harness on any hard failure.
int main(int argc, char** argv)
{
    argparse::ArgumentParser program("conntest-harness", "1.0", argparse::default_arguments::help);
    NES::ConnTest::configureArgumentParser(program);
    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << '\n' << program;
        return 2;
    }

    auto channel = NES::ConnTest::ControlChannel::connect(program.get<std::string>("--control-sock"));
    if (not channel)
    {
        return 1;
    }

    if (program.get<std::string>("--mode") == "source")
    {
        return NES::ConnTest::runSource(program, channel.value());
    }
    return NES::ConnTest::runSink(program, channel.value());
}
