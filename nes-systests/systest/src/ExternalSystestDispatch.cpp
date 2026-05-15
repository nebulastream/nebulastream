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

#include <ExternalSystestDispatch.hpp>

#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <vector>

#include <fmt/format.h>

namespace NES::Systest
{

namespace
{

/// Read the absolute path of the running binary. Used so the bats wrapper
/// can rebuild the systest container image from the same binary that was
/// just invoked — no need for the caller to point at $<TARGET_FILE:systest>.
std::optional<std::filesystem::path> currentExecutablePath()
{
    std::error_code ec;
    auto path = std::filesystem::read_symlink("/proc/self/exe", ec);
    if (ec)
    {
        return std::nullopt;
    }
    return path;
}

/// Parse a single header directive value (e.g., `# profile-dir: <value>`).
/// Returns nullopt if the directive is absent before the first non-comment,
/// non-blank line. The token after the directive prefix is taken verbatim
/// up to end-of-line (so paths with spaces work after a quote is stripped).
std::optional<std::string> readHeaderDirective(const std::filesystem::path& testFile, std::string_view prefix)
{
    std::ifstream stream(testFile);
    if (!stream.is_open())
    {
        return std::nullopt;
    }
    std::string line;
    while (std::getline(stream, line))
    {
        const auto firstNonSpace = line.find_first_not_of(" \t");
        if (firstNonSpace == std::string::npos)
        {
            continue;
        }
        if (line[firstNonSpace] != '#')
        {
            return std::nullopt;
        }
        if (!line.starts_with(prefix))
        {
            continue;
        }
        auto remainder = std::string_view(line).substr(prefix.size());
        const auto start = remainder.find_first_not_of(" \t");
        if (start == std::string_view::npos)
        {
            continue;
        }
        remainder.remove_prefix(start);
        const auto end = remainder.find_last_not_of(" \t");
        if (end != std::string_view::npos)
        {
            remainder = remainder.substr(0, end + 1);
        }
        if (!remainder.empty() && remainder.front() == '"' && remainder.back() == '"' && remainder.size() >= 2)
        {
            remainder.remove_prefix(1);
            remainder.remove_suffix(1);
        }
        return std::string(remainder);
    }
    return std::nullopt;
}

/// Locate the profile directory for `testFile`. Two sources, first wins:
///   1. A `# profile-dir: <path>` directive in the test file header. Paths
///      may be absolute or relative to the .test file's parent directory.
///   2. The convention `<plugin>/systest-profile`, derived as
///      dirname(dirname(testFile)) / "systest-profile" — matches the
///      `<plugin>/systests/X.test` + `<plugin>/systest-profile/` layout the
///      design documents.
/// Returns nullopt when neither yields a directory containing
/// `compose.snippet.yaml`.
std::optional<std::filesystem::path> findProfileDir(const std::filesystem::path& testFile)
{
    const auto testDir = testFile.parent_path();

    if (auto override = readHeaderDirective(testFile, "# profile-dir:"))
    {
        std::filesystem::path candidate = *override;
        if (candidate.is_relative())
        {
            candidate = testDir / candidate;
        }
        std::error_code ec;
        candidate = std::filesystem::weakly_canonical(candidate, ec);
        if (!ec && std::filesystem::is_regular_file(candidate / "compose.snippet.yaml"))
        {
            return candidate;
        }
        return std::nullopt;
    }

    const auto pluginRoot = testDir.parent_path();
    if (pluginRoot.empty())
    {
        return std::nullopt;
    }
    const auto convention = pluginRoot / "systest-profile";
    if (std::filesystem::is_regular_file(convention / "compose.snippet.yaml"))
    {
        std::error_code ec;
        auto canonical = std::filesystem::weakly_canonical(convention, ec);
        if (!ec)
        {
            return canonical;
        }
        return convention;
    }
    return std::nullopt;
}

void setEnvOrDie(const char* key, const std::string& value)
{
    if (::setenv(key, value.c_str(), /*overwrite=*/1) != 0)
    {
        std::cerr << fmt::format("systest: failed to setenv {}: {}\n", key, std::strerror(errno));
        std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
    }
}
}

DispatchOutcome dispatchExternalSystest(const std::filesystem::path& testFile, const std::vector<std::string>& requirements)
{
    DispatchOutcome out;

    if (requirements.empty())
    {
        out.skipReason = "no `# requires:` directives in test header";
        return out;
    }

    auto absoluteTestFile = testFile;
    {
        std::error_code ec;
        auto canonical = std::filesystem::weakly_canonical(absoluteTestFile, ec);
        if (!ec)
        {
            absoluteTestFile = canonical;
        }
    }

    constexpr std::string_view repoRoot = NES_REPO_ROOT;
    constexpr std::string_view batsLib = NES_BATS_LIB_PATH;
    constexpr std::string_view batsWrapper = EXTERNAL_SYSTEST_BATS;
    constexpr std::string_view defaultTmpDir = DEFAULT_NES_TEST_TMP_DIR;
    constexpr std::string_view defaultRuntimeImage = DEFAULT_NES_RUNTIME_BASE_IMAGE;

    if (!std::filesystem::is_regular_file(batsWrapper))
    {
        out.skipReason = fmt::format("external_systest bats wrapper not found at {}", batsWrapper);
        return out;
    }

    const auto profileDir = findProfileDir(absoluteTestFile);
    if (!profileDir)
    {
        out.skipReason = fmt::format(
            "could not locate a systest-profile directory for {} (looked for `# profile-dir:` directive and the "
            "`<plugin>/systest-profile/` convention)",
            absoluteTestFile.string());
        return out;
    }

    /// The bats runner needs a single satisfied profile to invoke the
    /// inner systest with `--accept-requires`. Use the first declared
    /// `# requires:` — tests with multiple profiles still work because
    /// the docker-compose snippet brings up everything the profile needs;
    /// the name is only used for resource scoping and the gate check.
    const auto& profileName = requirements.front();

    const auto systestBinary = currentExecutablePath();
    if (!systestBinary)
    {
        out.skipReason = "failed to resolve /proc/self/exe for the systest binary";
        return out;
    }

    const std::string runtimeImage = [defaultRuntimeImage]() -> std::string
    {
        if (const char* env = std::getenv("NES_RUNTIME_BASE_IMAGE"))
        {
            return env;
        }
        return std::string{defaultRuntimeImage};
    }();
    if (runtimeImage.empty())
    {
        out.skipReason
            = "NES_RUNTIME_BASE_IMAGE is unset and no default was baked in at build time. Rebuild with "
              "ENABLE_DOCKER_TESTS=ON, or set NES_RUNTIME_BASE_IMAGE in the environment.";
        return out;
    }

    const std::string tmpDir = [defaultTmpDir]() -> std::string
    {
        if (const char* env = std::getenv("NES_TEST_TMP_DIR"))
        {
            return env;
        }
        return std::string{defaultTmpDir};
    }();

    const std::string nesDir = [repoRoot]() -> std::string
    {
        if (const char* env = std::getenv("NES_DIR"))
        {
            return env;
        }
        return std::string{repoRoot};
    }();

    std::error_code ec;
    std::filesystem::create_directories(tmpDir, ec); /// ignore failure; bats will surface a clearer error

    std::cout << fmt::format(
        "Dispatching `# requires: {}` test to external_systest bats wrapper.\n"
        "  test file:    {}\n"
        "  profile dir:  {}\n"
        "  bats wrapper: {}\n"
        "  runtime img:  {}\n",
        profileName,
        absoluteTestFile.string(),
        profileDir->string(),
        std::string{batsWrapper},
        runtimeImage);
    std::cout.flush();

    setEnvOrDie("NES_DIR", nesDir);
    setEnvOrDie("NES_BATS_LIB", std::string{batsLib});
    setEnvOrDie("NES_SYSTEST", systestBinary->string());
    setEnvOrDie("NES_RUNTIME_BASE_IMAGE", runtimeImage);
    setEnvOrDie("NES_TEST_TMP_DIR", tmpDir);
    setEnvOrDie("PROFILE_DIR", profileDir->string());
    setEnvOrDie("PROFILE_NAME", profileName);
    setEnvOrDie("TEST_FILE", absoluteTestFile.string());

    const std::string wrapper{batsWrapper};
    std::array<const char*, 5> argv{"bats", "--verbose-run", "--timing", wrapper.c_str(), nullptr};

    /// Fork rather than execvp directly: in-progress systest output is
    /// already on stdout/stderr, and we want a clean handoff with the
    /// dispatched-to-bats banner above it visible in CLion's run window.
    const pid_t pid = ::fork();
    if (pid < 0)
    {
        out.skipReason = fmt::format("fork failed: {}", std::strerror(errno));
        return out;
    }
    if (pid == 0)
    {
        ::execvp("bats", const_cast<char* const*>(argv.data()));
        std::cerr << fmt::format("systest: failed to exec bats: {}\n", std::strerror(errno));
        std::_Exit(127);
    }

    int status = 0;
    if (::waitpid(pid, &status, 0) < 0)
    {
        out.skipReason = fmt::format("waitpid failed: {}", std::strerror(errno));
        return out;
    }

    out.dispatched = true;
    if (WIFEXITED(status))
    {
        out.exitCode = WEXITSTATUS(status);
    }
    else if (WIFSIGNALED(status))
    {
        out.exitCode = 128 + WTERMSIG(status);
    }
    else
    {
        out.exitCode = EXIT_FAILURE;
    }
    return out;
}

}
