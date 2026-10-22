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

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <utility>

#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Strings.hpp>
#include <ControlChannel.hpp>
#include <SourceSession.hpp>

#include <argparse/argparse.hpp>
#include <boost/system/system_error.hpp>

namespace NES::ConnTest
{
namespace
{

enum class ExitCode : std::uint8_t
{
    Ok = 0,
    ChannelError = 1,
    BadArguments = 2,
};

void configureArgumentParser(argparse::ArgumentParser& program)
{
    program.add_argument("--mode").required().choices("source").help("which stepper to run");
    program.add_argument("--config").required().help("SQL snippet (CREATE LOGICAL SOURCE + CREATE PHYSICAL SOURCE)");
    program.add_argument("--control-sock").required().help("AF_UNIX path of the runner's control channel");
    program.add_argument("--step-timeout-ms").help("per-step connector-work cap in milliseconds");
    program.add_argument("--buffer-size").help("per-buffer size in bytes");
    program.add_argument("--buffer-count").help("number of pooled buffers");
    program.add_argument("--observed-path").help("write the bytes observed per FILL here (runner decode)");
}

/// An absent flag leaves `target` at its default.
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

ExitCode runSource(const argparse::ArgumentParser& program, ControlChannel& channel)
{
    SourceSessionOptions options;
    options.configFile = program.get<std::string>("--config");

    auto stepTimeoutMs = static_cast<std::uint64_t>(options.stepTimeout.count());
    if (not applyUnsigned(program, "--buffer-size", options.bufferSize) or not applyUnsigned(program, "--buffer-count", options.bufferCount)
        or not applyUnsigned(program, "--step-timeout-ms", stepTimeoutMs))
    {
        return ExitCode::BadArguments;
    }
    options.stepTimeout = std::chrono::milliseconds{stepTimeoutMs};

    if (const auto observed = program.present<std::string>("--observed-path"))
    {
        options.observedPath = std::filesystem::path{*observed};
    }
    return runSourceSession(options, channel) ? ExitCode::Ok : ExitCode::ChannelError;
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

    return std::to_underlying(NES::ConnTest::runSource(program, *channel));
}
