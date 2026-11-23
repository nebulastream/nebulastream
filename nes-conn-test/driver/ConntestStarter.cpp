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

/// conntest-harness runs the connector side of a connection test, one command from the runner at a time.
/// Commands arrive on a socket, so stdout and stderr stay free for logs and sanitizer output.

#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Strings.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <Reply.hpp>
#include <SinkSession.hpp>
#include <SourceSession.hpp>

#include <argparse/argparse.hpp>
#include <boost/system/system_error.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

namespace NES::ConnTest
{
namespace
{

enum class ExitCode : std::uint8_t
{
    Ok = 0,
    ChannelError = 1,
    BadArguments = 2,
    Stuck = STUCK_EXIT_CODE,
};

void configureArgumentParser(argparse::ArgumentParser& program)
{
    program.add_argument("--mode").required().help("which stepper to run, source or sink");
    program.add_argument("--config").required().help("SQL snippet (CREATE LOGICAL SOURCE + CREATE PHYSICAL SOURCE, or CREATE SINK)");
    program.add_argument("--control-sock").required().help("AF_UNIX path of the runner's control channel");
    program.add_argument("--buffer-size").help("per-buffer size in bytes");
    program.add_argument("--buffer-count").help("number of pooled buffers");
    program.add_argument("--observed-path").help("write the bytes observed per FILL here (runner decode)");
    program.add_argument("--input-path").help("sink: the JSONL records that the sink writes");
    program.add_argument("--threads").help("sink: how many worker threads the engine runs");
}

/// An absent flag leaves `target` at its default.
/// The flag is parsed as a 64-bit number, so that the option's own bounds reject a value that its type cannot hold.
template <typename Option>
bool applyOption(const argparse::ArgumentParser& program, const std::string& flag, Option& target)
{
    const auto raw = program.present<std::string>(flag);
    if (not raw)
    {
        return true;
    }
    const auto parsed = NES::from_chars<std::uint64_t>(*raw);
    if (not parsed)
    {
        std::cerr << "error: " << flag << " expects an unsigned integer, got '" << *raw << "'\n";
        return false;
    }
    try
    {
        target = Option{*parsed};
    }
    catch (const Exception& ex)
    {
        std::cerr << "error: " << flag << ": " << trimWhiteSpaces(ex.what()) << "\n";
        return false;
    }
    return true;
}

ExitCode runSource(const argparse::ArgumentParser& program, ControlChannel& channel)
{
    SourceSessionOptions options;
    options.configFile = program.get<std::string>("--config");

    if (not applyOption(program, "--buffer-size", options.bufferSize) or not applyOption(program, "--buffer-count", options.bufferCount))
    {
        return ExitCode::BadArguments;
    }

    if (const auto observed = program.present<std::string>("--observed-path"))
    {
        options.observedPath = std::filesystem::path{*observed};
    }
    return runSourceSession(options, channel) ? ExitCode::Ok : ExitCode::ChannelError;
}

ExitCode runSink(const argparse::ArgumentParser& program, ControlChannel& channel)
{
    SinkSessionOptions options;
    options.configFile = program.get<std::string>("--config");

    if (not applyOption(program, "--buffer-size", options.bufferSize) or not applyOption(program, "--threads", options.threads))
    {
        return ExitCode::BadArguments;
    }

    if (const auto input = program.present<std::string>("--input-path"))
    {
        options.inputPath = std::filesystem::path{*input};
    }
    return runSinkSession(options, channel) ? ExitCode::Ok : ExitCode::ChannelError;
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
        return std::to_underlying(NES::ConnTest::ExitCode::BadArguments);
    }
    const auto kind = NES::ConnTest::parseConnectorKind(program.get<std::string>("--mode"));
    if (!kind.has_value())
    {
        std::cerr << fmt::format(
            "conntest-harness: --mode {} is not one of {}\n",
            program.get<std::string>("--mode"),
            fmt::join(magic_enum::enum_names<NES::ConnTest::ConnectorKind>(), ", "));
        return std::to_underlying(NES::ConnTest::ExitCode::BadArguments);
    }

    /// Connectors are registered at runtime, and binding and OPEN look them up by type.
    NES::loadBuiltinPlugins();

    /// Only the connect is in the try block, so an exception from the session is not reported as a channel error.
    const auto controlSock = program.get<std::string>("--control-sock");
    std::optional<NES::ConnTest::ControlChannel> channel;
    try
    {
        channel.emplace(controlSock);
    }
    catch (const boost::system::system_error& error)
    {
        std::cerr << "conntest-harness: cannot connect to the control socket " << controlSock << ": " << error.code().message() << '\n';
        return std::to_underlying(NES::ConnTest::ExitCode::ChannelError);
    }

    switch (*kind)
    {
        case NES::ConnTest::ConnectorKind::Source:
            return std::to_underlying(NES::ConnTest::runSource(program, *channel));
        case NES::ConnTest::ConnectorKind::Sink:
            return std::to_underlying(NES::ConnTest::runSink(program, *channel));
    }
    std::unreachable();
}
