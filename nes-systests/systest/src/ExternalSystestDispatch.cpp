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
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ranges>
#include <string>
#include <string_view>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unordered_set>
#include <vector>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt for range formatting

#include <SystestConfiguration.hpp>

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

    /// Predict the same log path the executor's setupLogging would have
    /// produced (PATH_TO_BINARY_DIR/nes-systests/SystemTest_<ts>_<pid>.log) so
    /// the user sees a stable, populated host log file after the run. The
    /// bats wrapper writes the inner systest's log to this path on teardown
    /// via `docker compose cp`. We compute it here — not in the inner — so
    /// it lives outside the docker container that gets torn down.
    const std::filesystem::path logDir = std::filesystem::path(NES_REPO_ROOT) / ".." / "nes-systests-logs";
    const std::filesystem::path hostLogDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests";
    std::filesystem::create_directories(hostLogDir, ec);
    const auto now = std::chrono::system_clock::now();
    const auto logPid = ::getpid();
    const std::string logFileName = fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{}.log", now, logPid);
    const std::filesystem::path hostLogPath = hostLogDir / logFileName;

    std::cout << fmt::format(
        "Dispatching `# requires: {}` test to external_systest bats wrapper.\n"
        "  test file:    {}\n"
        "  profile dir:  {}\n"
        "  bats wrapper: {}\n"
        "  runtime img:  {}\n"
        "Find the log at: file://{}\n",
        profileName,
        absoluteTestFile.string(),
        profileDir->string(),
        std::string{batsWrapper},
        runtimeImage,
        hostLogPath.string());
    std::cout.flush();

    setEnvOrDie("NES_DIR", nesDir);
    setEnvOrDie("NES_BATS_LIB", std::string{batsLib});
    setEnvOrDie("NES_SYSTEST", systestBinary->string());
    setEnvOrDie("NES_RUNTIME_BASE_IMAGE", runtimeImage);
    setEnvOrDie("NES_TEST_TMP_DIR", tmpDir);
    setEnvOrDie("PROFILE_DIR", profileDir->string());
    setEnvOrDie("PROFILE_NAME", profileName);
    setEnvOrDie("TEST_FILE", absoluteTestFile.string());
    setEnvOrDie("NES_DISPATCH_LOG_PATH", hostLogPath.string());

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

std::vector<std::string> readRequirementsFromHeader(const std::filesystem::path& testFile)
{
    std::vector<std::string> requirements;
    std::ifstream stream(testFile);
    if (!stream.is_open())
    {
        return requirements;
    }
    constexpr std::string_view directive = "# requires:";
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
            break;
        }
        if (!line.starts_with(directive))
        {
            continue;
        }
        auto remainder = std::string_view(line).substr(directive.size());
        const auto start = remainder.find_first_not_of(" \t");
        if (start == std::string_view::npos)
        {
            continue;
        }
        remainder.remove_prefix(start);
        const auto end = remainder.find_first_of(" \t");
        if (end != std::string_view::npos)
        {
            remainder = remainder.substr(0, end);
        }
        if (!remainder.empty())
        {
            requirements.emplace_back(remainder);
        }
    }
    return requirements;
}

void maybeDispatchExternalSystest(const SystestConfiguration& config)
{
    const auto& testFilePath = config.directlySpecifiedTestFiles.getValue();
    if (testFilePath.empty())
    {
        return;
    }

    const auto requirements = readRequirementsFromHeader(testFilePath);
    if (requirements.empty())
    {
        return;
    }

    /// If every declared requirement is already listed in --accept-requires,
    /// we're being invoked *by* the bats wrapper (recursion guard) and should
    /// fall through to the normal executor path.
    std::unordered_set<std::string> accepted;
    for (const auto& option : config.acceptRequires.getValues())
    {
        accepted.insert(option.getValue());
    }
    const bool allAccepted = std::ranges::all_of(requirements, [&](const auto& req) { return accepted.contains(req); });
    if (allAccepted)
    {
        return;
    }

    const auto outcome = dispatchExternalSystest(testFilePath, requirements);
    if (outcome.dispatched)
    {
        std::exit(outcome.exitCode); ///NOLINT(concurrency-mt-unsafe)
    }

    std::cerr << fmt::format(
        "Refusing to run file://{}: this test declares `# requires: {:}` and dispatch to the external_systest "
        "bats runner failed: {}.\n"
        "\n"
        "Expected runners, in order of preference:\n"
        "  1. Click the systest play-button in CLion (after applying the systest-profile convention so this "
        "     dispatcher can locate the profile dir automatically).\n"
        "  2. Run the ctest entry produced by `add_external_systest_profile(NAME ... TEST_FILE ... PROFILE_DIR "
        "...)` in the plugin's CMakeLists.txt. See nes-plugins/Sources/MQTTSource/CMakeLists.txt for an example.\n"
        "  3. Invoke scripts/testing/external_systest.bats directly with PROFILE_DIR / PROFILE_NAME / TEST_FILE / "
        "NES_DIR / NES_SYSTEST / NES_RUNTIME_BASE_IMAGE set.\n",
        std::filesystem::path(testFilePath).string(),
        requirements,
        outcome.skipReason);
    std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
}

}
