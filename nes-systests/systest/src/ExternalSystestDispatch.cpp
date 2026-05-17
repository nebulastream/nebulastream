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

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include <grp.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>
/// NOLINTNEXTLINE(misc-include-cleaner): provides CPPTRACE_TRY / CPPTRACE_CATCH used below; clang-tidy doesn't track macro-only usage.
#include <cpptrace/from_current.hpp>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <fmt/format.h>
#include <fmt/ranges.h> ///NOLINT: required by fmt for range formatting
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/emit.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>

/// This TU is the systest-side bridge into POSIX (fork/exec/pipe/recv/socket,
/// inherited env via getenv, raw signed flag constants, sockaddr reinterpret-
/// casts, short loop indices for sysadmin-style iteration). The following
/// advisory checks are silenced file-wide for that domain — they trigger on
/// every reasonable systems C++ TU and the alternatives (rewriting around
/// `std::getenv`, gsl::span over POSIX `int[2]`, renaming `e`/`rc`/`a` lambda
/// params, extracting two-digit literal port offsets to constants) would not
/// improve clarity. Targeted, behaviour-relevant warnings are still surfaced
/// case-by-case below.
/// NOLINTBEGIN(concurrency-mt-unsafe, cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-type-const-cast, cppcoreguidelines-pro-bounds-constant-array-index, hicpp-signed-bitwise, readability-identifier-length, readability-magic-numbers, misc-include-cleaner, fuchsia-default-arguments-declarations)
namespace NES::Systest
{

namespace
{

/// Parse a single header directive value (e.g., `# profile-dir: <value>`).
/// Returns nullopt if the directive is absent before the first non-comment,
/// non-blank line. Token after the prefix is taken verbatim up to
/// end-of-line, with optional surrounding double quotes stripped.
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
        if (remainder.size() >= 2 && remainder.front() == '"' && remainder.back() == '"')
        {
            remainder.remove_prefix(1);
            remainder.remove_suffix(1);
        }
        return std::string(remainder);
    }
    return std::nullopt;
}

/// Locate the profile dir for `testFile`. `# profile-dir:` directive wins;
/// otherwise the convention `<plugin>/systest-profile` (= dirname(dirname(
/// testFile)) / "systest-profile"). Profile dir must contain
/// compose.snippet.yaml — that's the file `docker compose -f` consumes.
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
        return ec ? convention : canonical;
    }
    return std::nullopt;
}

struct ProfileEndpoint
{
    std::string name;
    int containerPort = 0;
};

struct ProfileSpec
{
    std::string name;
    std::vector<ProfileEndpoint> endpoints;
};

/// Profile + endpoint names become env-var suffixes (`<NAME>_PORT`,
/// `NES_EXTERNAL_ENDPOINT_<NAME>`), so they have to be valid identifier
/// stems. The same restriction also keeps them sane as `# requires:` tokens
/// and ctest entry-name fragments.
bool isValidProfileIdentifier(std::string_view name)
{
    if (name.empty() || !(std::isalpha(static_cast<unsigned char>(name.front())) != 0))
    {
        return false;
    }
    return std::ranges::all_of(name, [](char c) { return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_'; });
}

/// Read `profile.yaml` from the profile dir. The manifest declares the
/// profile's `# requires:` name and the list of endpoints the dispatcher
/// must allocate host ports for. The dispatcher uses this to stay generic
/// — the only plugin-aware bit (which env var the source registrar reads)
/// stays in the plugin itself.
ProfileSpec readProfileSpec(const std::filesystem::path& profileDir)
{
    const auto manifest = profileDir / "profile.yaml";
    if (!std::filesystem::is_regular_file(manifest))
    {
        throw TestException(
            "external_systest: missing profile.yaml in {} (expected `name:` and `endpoints:` declarations)", profileDir.string());
    }
    YAML::Node root;
    try
    {
        root = YAML::LoadFile(manifest.string());
    }
    catch (const YAML::Exception& exception)
    {
        throw TestException("external_systest: failed to parse {}: {}", manifest.string(), exception.what());
    }
    if (!root.IsMap() || !root["name"] || !root["name"].IsScalar())
    {
        throw TestException("external_systest: {} must declare a scalar `name:` field", manifest.string());
    }
    ProfileSpec spec;
    spec.name = root["name"].as<std::string>();
    if (!isValidProfileIdentifier(spec.name))
    {
        throw TestException(
            "external_systest: {} `name: {}` must match [A-Za-z][A-Za-z0-9_]* (used as a `# requires:` token and a ctest entry-name "
            "suffix)",
            manifest.string(),
            spec.name);
    }
    if (!root["endpoints"] || !root["endpoints"].IsSequence())
    {
        throw TestException("external_systest: {} must declare an `endpoints:` sequence (at least one)", manifest.string());
    }
    std::unordered_set<std::string> seenEndpoints;
    for (const auto& node : root["endpoints"])
    {
        if (!node.IsMap() || !node["name"] || !node["container_port"])
        {
            throw TestException(
                "external_systest: {} endpoint entry must have `name:` and `container_port:` fields (got: `{}`)",
                manifest.string(),
                YAML::Dump(node));
        }
        auto endpointName = node["name"].as<std::string>();
        const auto containerPort = node["container_port"].as<int>();
        if (!isValidProfileIdentifier(endpointName))
        {
            throw TestException(
                "external_systest: {} endpoint `name: {}` must match [A-Za-z][A-Za-z0-9_]* (used as the suffix in `<NAME>_PORT` / "
                "`<NAME>_CONTAINER_PORT` / `NES_EXTERNAL_ENDPOINT_<NAME>` env vars)",
                manifest.string(),
                endpointName);
        }
        if (containerPort < 1 || containerPort > 65535)
        {
            throw TestException(
                "external_systest: {} endpoint `{}` declares `container_port: {}`, must be in [1, 65535]",
                manifest.string(),
                endpointName,
                containerPort);
        }
        if (!seenEndpoints.insert(endpointName).second)
        {
            throw TestException(
                "external_systest: {} declares endpoint `{}` more than once (duplicate names would silently overwrite each other's env "
                "vars)",
                manifest.string(),
                endpointName);
        }
        spec.endpoints.push_back({.name = std::move(endpointName), .containerPort = containerPort});
    }
    if (spec.endpoints.empty())
    {
        throw TestException("external_systest: {} declares no endpoints", manifest.string());
    }
    return spec;
}

/// Address the in-process test should use to reach the dispatcher-managed
/// broker. Two cases:
///   - systest binary runs on the host directly → 127.0.0.1 (loopback).
///   - systest binary runs inside the CLion dev-container toolchain → the
///     host's docker daemon publishes the broker on the host's interfaces;
///     the dev container reaches them via `host.docker.internal`.
///
/// On Linux `host.docker.internal` requires `--add-host=host.docker.internal:
/// host-gateway` on the *dev container* (Docker Desktop / Mac do this
/// automatically). `.claude/skills/nes-build/in-docker.sh` and the install
/// script's printed CLion run-options example both include that flag; the
/// reachability probe below surfaces a precise diagnostic if either is
/// missing.
std::string computeExternalHost()
{
    if (const char* override = std::getenv("NES_EXTERNAL_HOST_OVERRIDE"))
    {
        return override;
    }
    if (std::filesystem::exists("/.dockerenv"))
    {
        return "host.docker.internal";
    }
    return "127.0.0.1";
}

/// Connect once to (host, port) with a short timeout. Returns true on
/// successful TCP handshake, false on any failure (timeout, refused,
/// unreachable). Used to fail fast after compose-up rather than letting the
/// MQTT publisher's 30s ready-timeout swallow a misconfigured dev container.
bool probeReachable(const std::string& host, int port, std::chrono::milliseconds timeout)
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    const auto portStr = std::to_string(port);
    if (::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &result) != 0)
    {
        return false;
    }
    bool ok = false;
    for (const addrinfo* rp = result; rp != nullptr; rp = rp->ai_next)
    {
        const int sock = ::socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK, rp->ai_protocol);
        if (sock < 0)
        {
            continue;
        }
        const int rc = ::connect(sock, rp->ai_addr, rp->ai_addrlen);
        if (rc == 0)
        {
            ok = true;
            ::close(sock);
            break;
        }
        if (errno == EINPROGRESS)
        {
            pollfd pfd{.fd = sock, .events = POLLOUT, .revents = 0};
            if (::poll(&pfd, 1, static_cast<int>(timeout.count())) > 0 && (pfd.revents & POLLOUT) != 0)
            {
                int sockErr = 0;
                socklen_t len = sizeof(sockErr);
                if (::getsockopt(sock, SOL_SOCKET, SO_ERROR, &sockErr, &len) == 0 && sockErr == 0)
                {
                    ok = true;
                    ::close(sock);
                    break;
                }
            }
        }
        ::close(sock);
    }
    ::freeaddrinfo(result);
    return ok;
}

/// Verify the current process can reach the docker socket before we start
/// shelling out to `docker volume create` / `docker compose up`. The
/// alternative is a five-line cpptrace dump triggered ~four call frames in
/// when the first docker subprocess fails with "permission denied while
/// trying to connect to the docker API at unix:///var/run/docker.sock" —
/// usable but not very actionable.
///
/// Most common cause of the failure: CLion's docker toolchain passes
/// `--user $UID:$GID` to map host UID/GID for file ownership. Docker
/// silently drops the supplementary groups the image baked into `/etc/group`
/// when `--user` overrides USER, so even though the image lists the docker
/// (or systemd-network squatter) GID for the user, the running process is
/// not a member at run time. The fix is `--group-add <gid>` in the
/// toolchain's run options. The diagnostic below tells the user the exact
/// GID to add and where to add it.
///
/// We probe by stat()-ing the socket and walking the process's
/// supplementary groups. A miss throws CannotOpenSource with the targeted
/// hint; a hit (or socket-not-at-standard-path, e.g. rootless docker) lets
/// dispatch proceed and any subsequent docker error surface its own way.
void checkDockerSocketAccess()
{
    constexpr const char* socketPath = "/var/run/docker.sock";
    struct stat statBuf{};
    if (::stat(socketPath, &statBuf) != 0)
    {
        /// Standard path missing — rootless docker, daemon not running, etc.
        /// Let docker's own error surface from the next subprocess call.
        return;
    }
    const gid_t socketGid = statBuf.st_gid;

    if (::getegid() == socketGid)
    {
        return;
    }
    std::array<gid_t, 1024> supplementary{};
    const int nGroups = ::getgroups(static_cast<int>(supplementary.size()), supplementary.data());
    if (nGroups < 0)
    {
        /// getgroups failed — bail rather than throw a confusing diagnostic;
        /// the docker subprocess will fail with its own error if it has to.
        return;
    }
    for (int i = 0; i < nGroups; ++i)
    {
        if (supplementary[i] == socketGid)
        {
            return;
        }
    }

    std::string groupName = "(no name in /etc/group)";
    if (const auto* grp = ::getgrgid(socketGid); grp != nullptr && grp->gr_name != nullptr)
    {
        groupName = grp->gr_name;
    }

    /// ANSI escapes only when stderr is a TTY — build logs (file/pipe) get
    /// plain text instead of literal `\033[…m` garbage.
    const bool useColor = ::isatty(STDERR_FILENO) != 0;
    const auto* const red = useColor ? "\033[1;31m" : "";
    const auto* const yellow = useColor ? "\033[1;33m" : "";
    const auto* const bold = useColor ? "\033[1m" : "";
    const auto* const reset = useColor ? "\033[0m" : "";

    /// Banner-style framing so the actionable hint pops in a CLion build
    /// window without the user having to scroll. We `std::exit` rather than
    /// throw to suppress the cpptrace terminate-handler stack dump that
    /// would otherwise push the diagnostic off-screen — at this point in
    /// dispatch no RAII guards have been constructed yet, so cleanup is a
    /// no-op.
    std::cerr << '\n'
              << red << "============================================================================\n"
              << " external_systest: cannot access the docker socket\n"
              << "============================================================================" << reset << "\n\n"
              << "The socket at " << bold << socketPath << reset << " is group-owned by GID " << bold << socketGid << reset << " (`"
              << groupName << "` inside this container).\n"
              << "The current process is uid=" << ::geteuid() << " gid=" << ::getegid() << " and is not a member of GID " << socketGid
              << ".\n\n"
              << "Most likely cause: CLion's docker toolchain passes " << bold << "--user $UID:$GID" << reset << " for\n"
              << "host-user file ownership, and Docker silently drops the supplementary\n"
              << "groups baked into /etc/group whenever --user is set.\n\n"
              << yellow << "  FIX:" << reset << " in CLion " << bold << "Settings -> Docker -> Container settings -> Run options" << reset
              << ",\n"
              << "       add the flag\n\n"
              << "           " << bold << "--group-add " << socketGid << reset << "\n\n"
              << "(.claude/skills/nes-build/in-docker.sh doesn't pass --user, so the\n"
              << "image bake is enough there — no flag needed for that path.)\n"
              << red << "============================================================================" << reset << "\n\n";
    std::cerr.flush();
    std::exit(EXIT_FAILURE); ///NOLINT(concurrency-mt-unsafe)
}

/// Ask the kernel for a free TCP port: bind a socket to port 0 on
/// 127.0.0.1, read back the assigned port, close. The port can in theory be
/// reused by another process before docker binds to it (TOCTOU); for
/// local-developer interactive use the window is small enough that we
/// accept the risk — the user explicitly asked to defer hardened
/// collision-handling.
int pickFreeTcpPort()
{
    const int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
    {
        throw CannotOpenSource("external_systest: socket() failed: {}", std::strerror(errno));
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        ::close(sock);
        throw CannotOpenSource("external_systest: bind() failed: {}", std::strerror(errno));
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) < 0)
    {
        ::close(sock);
        throw CannotOpenSource("external_systest: getsockname() failed: {}", std::strerror(errno));
    }
    const int port = ntohs(addr.sin_port);
    ::close(sock);
    return port;
}

/// Set an env var or throw. Used for the small set of vars the compose
/// snippet substitutes and the plugin registrars consume (PROFILE_VOLUME,
/// NES_EXTERNAL_HOST, the per-endpoint *_PORT / NES_EXTERNAL_ENDPOINT_*
/// pairs). Throws CannotOpenSource so the surrounding RAII guards
/// (ProfileVolumeGuard, ExternalSystestDispatchGuard) get a chance to clean
/// up before the process unwinds.
void setEnvOrThrow(const char* key, const std::string& value)
{
    if (::setenv(key, value.c_str(), /*overwrite=*/1) != 0)
    {
        throw CannotOpenSource("external_systest: setenv {} failed: {}", key, std::strerror(errno));
    }
}

/// Fork+exec+wait the docker binary with the given argv. argv must be
/// owned strings (typically `{"docker", "compose", ...}`) — the previous
/// shape that built std::string_view vectors directly from temporaries
/// produced a footgun where the .data() pointers outlived the temporaries.
/// Centralising argv lifetime here is what retires that trap.
///
/// If `captureStdout` is non-null, the child's stdout + stderr are piped
/// back into the string. Otherwise they inherit the parent's fds.
/// Returns the exit code; -1 if signal-killed.
int runDockerArgv(const std::vector<std::string>& argvOwned, std::string* captureStdout = nullptr)
{
    std::vector<char*> argv;
    argv.reserve(argvOwned.size() + 1);
    for (const auto& s : argvOwned)
    {
        argv.push_back(const_cast<char*>(s.c_str()));
    }
    argv.push_back(nullptr);

    /// NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): POSIX `pipe(int[2])` requires this shape.
    int pipeFds[2] = {-1, -1};
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay): pipe() takes int*; the decay is mandated by the POSIX signature.
    if ((captureStdout != nullptr) && ::pipe(pipeFds) != 0)
    {
        throw CannotOpenSource("external_systest: pipe() failed: {}", std::strerror(errno));
    }

    const pid_t pid = ::fork();
    if (pid < 0)
    {
        if (captureStdout != nullptr)
        {
            ::close(pipeFds[0]);
            ::close(pipeFds[1]);
        }
        throw CannotOpenSource("external_systest: fork() for {} failed: {}", argvOwned.front(), std::strerror(errno));
    }
    if (pid == 0)
    {
        if (captureStdout != nullptr)
        {
            ::close(pipeFds[0]);
            ::dup2(pipeFds[1], STDOUT_FILENO);
            ::dup2(pipeFds[1], STDERR_FILENO);
            ::close(pipeFds[1]);
        }
        ::execvp(argvOwned.front().c_str(), argv.data());
        std::cerr << fmt::format("external_systest: failed to exec {}: {}\n", argvOwned.front(), std::strerror(errno));
        std::_Exit(127);
    }
    if (captureStdout != nullptr)
    {
        ::close(pipeFds[1]);
        std::array<char, 4096> buffer{};
        while (true)
        {
            const auto n = ::read(pipeFds[0], buffer.data(), buffer.size());
            if (n > 0)
            {
                captureStdout->append(buffer.data(), static_cast<size_t>(n));
            }
            else if (n == 0 || errno != EINTR)
            {
                break;
            }
        }
        ::close(pipeFds[0]);
    }
    int status = 0;
    if (::waitpid(pid, &status, 0) < 0)
    {
        throw CannotOpenSource("external_systest: waitpid for {} failed: {}", argvOwned.front(), std::strerror(errno));
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/// Fork+exec a docker subcommand. argv must NOT include "docker" — that's
/// added here.
int runDocker(std::initializer_list<std::string_view> args)
{
    std::vector<std::string> argvOwned{"docker"};
    for (auto a : args)
    {
        argvOwned.emplace_back(a);
    }
    return runDockerArgv(argvOwned);
}

/// Spawn `docker compose -p <project> -f <snippet> <subcommand...>` and
/// wait. Caller is responsible for any env vars the compose file references
/// (PROFILE_VOLUME, NES_EXTERNAL_HOST, <NAME>_PORT, ...) being set before
/// this is called — they propagate via the child's inherited environment.
///
/// `--progress=quiet` collapses docker compose's per-tick spinner reprints
/// (dozens of "[+] up 1/2 ⠋ Container ... Waiting Ns" lines while
/// healthchecking) into nothing. Errors still print normally.
int runDockerCompose(const std::string& projectName, const std::filesystem::path& composeFile, std::initializer_list<std::string_view> args)
{
    std::vector<std::string> argvOwned{"docker", "compose", "--progress=quiet", "-p", projectName, "-f", composeFile.string()};
    for (auto a : args)
    {
        argvOwned.emplace_back(a);
    }
    return runDockerArgv(argvOwned);
}

/// Capture stdout+stderr of `docker compose -p <project> -f <snippet> logs`.
/// Used by the up-failure path to fold the broker container's output into
/// the exception message — by the time the dispatch guard's destructor
/// runs `docker compose down`, the per-container logs are gone. Capturing
/// before tear-down keeps the diagnostic actionable.
std::string captureDockerComposeLogs(const std::string& projectName, const std::filesystem::path& composeFile)
{
    const std::vector<std::string> argvOwned{
        "docker", "compose", "-p", projectName, "-f", composeFile.string(), "logs", "--no-color", "--no-log-prefix"};
    std::string output;
    try
    {
        runDockerArgv(argvOwned, &output);
    }
    catch (const std::exception& exception)
    {
        return fmt::format("(failed to capture docker compose logs: {})", exception.what());
    }
    return output;
}

/// Populate a docker volume with the contents of `sourceDir`. Works regardless
/// of whether `sourceDir` is on the host filesystem or only visible from
/// inside a dev container — `docker cp` reads from the *client's*
/// filesystem (wherever the systest binary runs) and streams the bytes to
/// the daemon, which writes them into the container the volume is mounted
/// in. No path translation needed, no shell pipe to fail silently.
///
/// The helper container exists because `docker cp` requires a container
/// (not a bare volume) as the destination. An `alpine sleep infinity`
/// holds the volume mounted at /dest while `docker cp` populates it, then
/// the HelperStopper RAII guard stops the helper.
void populateVolumeFromDir(const std::string& volumeName, const std::filesystem::path& sourceDir)
{
    const auto helperName
        = fmt::format("nes-systest-profile-loader-{}-{}", ::getpid(), std::chrono::steady_clock::now().time_since_epoch().count());
    if (const auto rc = runDocker({"run", "-d", "--rm", "--name", helperName, "-v", volumeName + ":/dest", "alpine", "sleep", "infinity"});
        rc != 0)
    {
        throw CannotOpenSource("external_systest: failed to start profile-loader helper (`docker run` exit {})", rc);
    }

    /// Ensure the helper goes away no matter how we leave the function.
    /// Destructor swallows any failure — best effort.
    struct HelperStopper
    {
        explicit HelperStopper(const std::string& n) : name(n) { }

        ~HelperStopper()
        {
            try
            {
                runDocker({"stop", "-t", "0", name});
            }
            /// NOLINTNEXTLINE(bugprone-empty-catch): destructor must not throw; intentional swallow.
            catch (const std::exception&)
            {
            }
        }

        HelperStopper(const HelperStopper&) = delete;
        HelperStopper& operator=(const HelperStopper&) = delete;
        HelperStopper(HelperStopper&&) = delete;
        HelperStopper& operator=(HelperStopper&&) = delete;

        /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): the const-ref is intentional — this is a local RAII guard whose lifetime cannot outlive the helperName it binds to, and the deleted copy/move ops above enforce it at compile time.
        const std::string& name;
    };

    const HelperStopper stopper(helperName);

    /// The `/.` suffix means "copy the directory's contents, not the
    /// directory itself" — so mosquitto.conf lands at /dest/mosquitto.conf,
    /// not /dest/<profile-dir-name>/mosquitto.conf.
    const auto src = sourceDir.string() + "/.";
    const auto dst = helperName + ":/dest";
    if (const auto rc = runDocker({"cp", src, dst}); rc != 0)
    {
        throw CannotOpenSource(
            "external_systest: failed to populate docker volume `{}` from {} (`docker cp` exit {})", volumeName, sourceDir.string(), rc);
    }
}

}

/// Owns the lifetime of an anonymous docker volume populated with the
/// profile-dir contents. Used by the broker container as the `/profile` mount
/// source — sidesteps the host-vs-container path mismatch a bind mount would
/// hit when systest runs inside the CLion docker toolchain.
class ProfileVolumeGuard
{
public:
    explicit ProfileVolumeGuard(const std::filesystem::path& profileDir)
        : volumeName(fmt::format("nes-systest-profile-{}-{}", ::getpid(), std::chrono::system_clock::now().time_since_epoch().count()))
    {
        if (const auto rc = runDocker({"volume", "create", volumeName}); rc != 0)
        {
            throw CannotOpenSource("external_systest: `docker volume create {}` exited with status {}", volumeName, rc);
        }
        created = true;

        CPPTRACE_TRY
        {
            populateVolumeFromDir(volumeName, profileDir);
        }
        CPPTRACE_CATCH(...)
        {
            /// Surface the original error, but try to clean up first.
            runDocker({"volume", "rm", volumeName});
            created = false;
            throw;
        }
    }

    ~ProfileVolumeGuard()
    {
        if (created)
        {
            try
            {
                runDocker({"volume", "rm", volumeName});
            }
            /// NOLINTNEXTLINE(bugprone-empty-catch): destructor must not throw; intentional swallow.
            catch (const std::exception&)
            {
            }
        }
    }

    ProfileVolumeGuard(const ProfileVolumeGuard&) = delete;
    ProfileVolumeGuard& operator=(const ProfileVolumeGuard&) = delete;
    ProfileVolumeGuard(ProfileVolumeGuard&&) = delete;
    ProfileVolumeGuard& operator=(ProfileVolumeGuard&&) = delete;

    [[nodiscard]] const std::string& name() const { return volumeName; }

private:
    std::string volumeName;
    bool created = false;
};

/// NOLINTBEGIN(readability-identifier-naming): trailing-underscore parameters are intentional — they
/// disambiguate from same-named members in the initializer list. Without them, `std::move(name)` would
/// move the parameter, and the next initializer reading the same identifier would be a use-after-move.
ExternalSystestDispatchGuard::ExternalSystestDispatchGuard(
    std::string projectName_, std::filesystem::path composeFile_, std::unique_ptr<ProfileVolumeGuard> volume_)
    : projectName(std::move(projectName_)), composeFile(std::move(composeFile_)), volume(std::move(volume_))
/// NOLINTEND(readability-identifier-naming)
{
    const auto rc = runDockerCompose(projectName, composeFile, {"up", "-d", "--wait"});
    if (rc != 0)
    {
        /// Snapshot the per-container logs *before* tearing the stack down —
        /// `docker compose down` removes the containers, after which their
        /// logs are gone. Folding them into the exception means the user
        /// sees mosquitto's actual stderr ("Unable to open config file
        /// ...", "Address already in use", etc.) without having to re-run
        /// anything.
        const auto containerLogs = captureDockerComposeLogs(projectName, composeFile);
        /// Bring anything partial down — otherwise a half-up stack leaks
        /// until the next test run reuses the project name. The volume
        /// guard's destructor (running after this throw unwinds) takes care
        /// of the docker volume.
        runDockerCompose(projectName, composeFile, {"down"});
        throw CannotOpenSource(
            "external_systest: `docker compose up` exited with status {} for {} (project {}).\n"
            "--- broker container logs ---\n"
            "{}"
            "--- end of logs ---",
            rc,
            composeFile.string(),
            projectName,
            containerLogs.empty() ? std::string("(no output captured)\n") : containerLogs);
    }
    up = true;
}

ExternalSystestDispatchGuard::~ExternalSystestDispatchGuard()
{
    if (up)
    {
        try
        {
            /// `down` without `-v` because the volume is external (declared
            /// `external: true` in the snippet) — compose treats it as
            /// owned by the caller and doesn't remove it. The volume guard
            /// destroys the volume itself when this dispatch guard's member
            /// destruction runs immediately after the body of this dtor.
            runDockerCompose(projectName, composeFile, {"down"});
        }
        /// NOLINTNEXTLINE(bugprone-empty-catch): destructor must not throw; intentional swallow.
        catch (const std::exception&)
        {
        }
    }
}

std::optional<std::string> readRequirementFromHeader(const std::filesystem::path& testFile)
{
    std::ifstream stream(testFile);
    if (!stream.is_open())
    {
        return std::nullopt;
    }
    constexpr std::string_view directive = "# requires:";
    std::optional<std::string> requirement;
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
        if (remainder.empty())
        {
            continue;
        }
        if (requirement)
        {
            throw TestException(
                "Test {} declares multiple `# requires:` directives (`{}` and `{}`). A test selects exactly one external profile; "
                "merge the services into a single profile if you need more than one.",
                testFile.string(),
                *requirement,
                std::string(remainder));
        }
        requirement = std::string(remainder);
    }
    return requirement;
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity): orchestrates the full external_systest dispatch flow (requirement parse → profile resolution → port allocation → compose-up → guard handoff). Splitting it would obscure the linear sequence the comments document; complexity is 27 vs threshold 25.
std::unique_ptr<ExternalSystestDispatchGuard> maybeDispatchExternalSystest(SystestConfiguration& config)
{
    const auto& testFilePath = config.directlySpecifiedTestFiles.getValue();
    if (testFilePath.empty())
    {
        return nullptr;
    }

    const auto requirement = readRequirementFromHeader(testFilePath);
    if (!requirement)
    {
        return nullptr;
    }

    /// Recursion / "already-set-up" guard: if the caller already declared the
    /// profile satisfied (e.g., a future ctest entry that wants to manage its
    /// own broker, or a developer who started one in a separate terminal),
    /// trust them and stay out of the way.
    for (const auto& option : config.acceptRequires.getValues())
    {
        if (option.getValue() == *requirement)
        {
            return nullptr;
        }
    }

    const auto& profileName = *requirement;

    const auto profileDir = findProfileDir(testFilePath);
    if (!profileDir)
    {
        throw CannotOpenSource(
            "external_systest: could not locate a systest-profile directory for {} (looked for `# profile-dir:` "
            "directive and the `<plugin>/systest-profile/` convention)",
            testFilePath);
    }

    const auto profileSpec = readProfileSpec(*profileDir);
    if (profileSpec.name != profileName)
    {
        throw TestException(
            "external_systest: profile name mismatch for {}: `# requires:` declares `{}` but {} declares `name: {}`",
            testFilePath,
            profileName,
            (*profileDir / "profile.yaml").string(),
            profileSpec.name);
    }

    const auto composeFile = *profileDir / "compose.snippet.yaml";

    /// Fail fast with a targeted hint if the process can't reach the docker
    /// socket — otherwise the failure cascades through `docker volume create`
    /// as a generic "permission denied" cpptrace deep in ProfileVolumeGuard.
    checkDockerSocketAccess();

    /// Stage the profile dir's contents into a docker volume rather than
    /// bind-mounting from the host filesystem. The dev container's view of
    /// `*profileDir` (/tmp/nebulastream-public/...) typically doesn't exist
    /// on the host docker daemon's filesystem, so a bind mount fails — the
    /// daemon mounts an empty directory and the broker can't find its
    /// config. A docker volume sidesteps the host-vs-container path
    /// mapping entirely: we tar-pipe the in-container files into the volume
    /// via a one-shot alpine helper, then the broker mounts the volume.
    auto volumeGuard = std::make_unique<ProfileVolumeGuard>(*profileDir);

    /// Per-pid project name so concurrent runs at least don't collide on
    /// docker's resource namespace (network, container names). Host ports
    /// can still collide if two tests pick the same random port — that's
    /// the documented trade-off.
    const auto projectName = fmt::format("nes-systest-{}-{}", profileName, ::getpid());

    /// Generic endpoint-port allocation, driven by profile.yaml. For every
    /// endpoint the profile declares, allocate a free host port and export:
    ///   <NAME>_PORT                    — allocated host port (compose
    ///                                    snippet substitutes
    ///                                    `${<NAME>_PORT}` into its `ports:`
    ///                                    mapping's host side).
    ///   <NAME>_CONTAINER_PORT          — container-side port declared in
    ///                                    profile.yaml. Snippet uses this for
    ///                                    the container side of the mapping
    ///                                    so profile.yaml stays the single
    ///                                    source of truth for the port a
    ///                                    given service listens on.
    ///   NES_EXTERNAL_ENDPOINT_<NAME>   — `<host>:<port>` pair (plugin
    ///                                    registrars read this; the source
    ///                                    config's serveruri / connection
    ///                                    string is rewritten from it).
    /// `NES_EXTERNAL_HOST` is the host address the in-process test reaches
    /// the broker at; 127.0.0.1 on the host, host.docker.internal inside the
    /// CLion dev-container toolchain.
    const std::string externalHost = computeExternalHost();
    setEnvOrThrow("PROFILE_VOLUME", volumeGuard->name());
    setEnvOrThrow("NES_EXTERNAL_HOST", externalHost);
    std::vector<std::pair<std::string, int>> allocations;
    allocations.reserve(profileSpec.endpoints.size());
    for (const auto& endpoint : profileSpec.endpoints)
    {
        const auto endpointUpper = NES::toUpperCase(endpoint.name);
        const int hostPort = pickFreeTcpPort();
        setEnvOrThrow(fmt::format("{}_PORT", endpointUpper).c_str(), std::to_string(hostPort));
        setEnvOrThrow(fmt::format("{}_CONTAINER_PORT", endpointUpper).c_str(), std::to_string(endpoint.containerPort));
        setEnvOrThrow(fmt::format("NES_EXTERNAL_ENDPOINT_{}", endpointUpper).c_str(), fmt::format("{}:{}", externalHost, hostPort));
        allocations.emplace_back(endpoint.name, hostPort);
    }

    std::cout << fmt::format(
        "Bringing up external_systest profile `{}` for {}\n"
        "  compose file:    {}\n"
        "  profile dir:     {}\n"
        "  profile volume:  {}\n"
        "  external host:   {}\n"
        "  endpoints:       {}\n"
        "  project name:    {}\n",
        profileName,
        testFilePath,
        composeFile.string(),
        profileDir->string(),
        volumeGuard->name(),
        externalHost,
        fmt::join(allocations | std::views::transform([](const auto& a) { return fmt::format("{}={}", a.first, a.second); }), ", "),
        projectName);
    std::cout.flush();

    auto guard = std::make_unique<ExternalSystestDispatchGuard>(projectName, composeFile, std::move(volumeGuard));

    /// Reachability probe — fail fast with a targeted diagnostic rather than
    /// letting the source's connect attempt block for tens of seconds. The
    /// in-container case hits this when the dev container was started without
    /// `--add-host=host.docker.internal:host-gateway`, which leaves
    /// host.docker.internal unresolvable. The host case hits this if the
    /// broker port is somehow not bound (unlikely after `--wait`).
    for (const auto& [name, port] : allocations)
    {
        if (!probeReachable(externalHost, port, std::chrono::seconds(2)))
        {
            /// ANSI escapes only when stderr is a TTY — CI build logs (file/pipe)
            /// get plain text instead of literal `\033[…m` garbage.
            const bool useColor = ::isatty(STDERR_FILENO) != 0;
            const auto* const red = useColor ? "\033[1;31m" : "";
            const auto* const yellow = useColor ? "\033[1;33m" : "";
            const auto* const bold = useColor ? "\033[1m" : "";
            const auto* const reset = useColor ? "\033[0m" : "";

            const auto isContainerCase = (externalHost == "host.docker.internal");

            /// Echo to stderr before throwing — the cpptrace terminate handler
            /// only prints stack frames, not the exception's what(), so without
            /// this CI logs would only show the trace. The throw still fires
            /// after for normal exception propagation.
            std::cerr << '\n'
                      << red << "============================================================================\n"
                      << " external_systest: broker came up but endpoint is unreachable\n"
                      << "============================================================================" << reset << "\n\n"
                      << "Profile " << bold << "`" << profileName << "`" << reset << " endpoint " << bold << "`" << name << "`" << reset
                      << " at " << bold << externalHost << ":" << port << reset << " did not respond within 2s.\n\n";
            if (isContainerCase)
            {
                std::cerr << "Cause: this process is running inside a container (`/.dockerenv` present),\n"
                          << "so the dispatcher uses `host.docker.internal` to reach the host-published\n"
                          << "broker port — but Docker silently makes `host.docker.internal` resolvable\n"
                          << "only when the container was started with `--add-host=host.docker.internal:\n"
                          << "host-gateway`. Three launchers do this for you:\n"
                          << "  - CLion docker toolchain run options (Settings → Docker → Container\n"
                          << "    settings → Run options): " << bold << "--add-host=host.docker.internal:host-gateway" << reset << "\n"
                          << "  - .claude/skills/nes-build/in-docker.sh (already includes the flag)\n"
                          << "  - .github/steps/run-in-container/action.yml (already includes the flag)\n\n"
                          << yellow << "  FIX:" << reset << " add " << bold << "--add-host=host.docker.internal:host-gateway" << reset
                          << " to whichever\n"
                          << "       launcher started this container.\n";
            }
            else
            {
                std::cerr << "Cause: the broker came up but its host-published port is not reachable\n"
                          << "at " << externalHost << ":" << port << ".\n\n"
                          << yellow << "  FIX:" << reset << " confirm the broker container is bound to the host's loopback\n"
                          << "       interface and that nothing on the host is firewalling that port.\n";
            }
            std::cerr << red << "============================================================================" << reset << "\n\n";
            std::cerr.flush();

            throw CannotOpenSource(
                "external_systest: profile `{}` came up but endpoint `{}` at {}:{} is unreachable", profileName, name, externalHost, port);
        }
    }

    /// Mark the gate as satisfied so SystestState's discovery filter and the
    /// directly-specified-file refusal path both let the test through.
    config.acceptRequires.add(*requirement);

    return guard;
}

}

/// NOLINTEND(concurrency-mt-unsafe, cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-type-const-cast, cppcoreguidelines-pro-bounds-constant-array-index, hicpp-signed-bitwise, readability-identifier-length, readability-magic-numbers, misc-include-cleaner, fuchsia-default-arguments-declarations)
