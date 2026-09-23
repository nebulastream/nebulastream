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

#include <Logging.hpp>

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <system_error>
#include <unistd.h>
#include <Config/Config.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <sys/stat.h>

namespace NES
{
namespace
{
/// A temp symlink (`latest.log.<pid>.<rand>.tmp`) is orphaned if the process is hard-killed
/// (OOM, CI cancel, signal) in the window between create_symlink() and rename() below. Because temp
/// names are unique, no later run ever reclaims another run's leftover, so dangling temps slowly
/// accumulate in the shared work dir. Sweep clearly-stale temps here.
///
/// Cleanup is scoped strictly by age, never by a blind `latest.log.*.tmp` name match: a live
/// in-flight temp exists only for the microseconds between create_symlink() and rename(), so any
/// temp older than the generous threshold below cannot belong to a concurrent agent mid-rename.
/// This keeps the concurrency guarantee intact. An age gate is also correct across PID
/// namespaces (containerized runners), where an "is the owning PID still alive?" check would misfire
/// on a colliding PID and could delete another agent's live temp.
constexpr std::time_t staleTempMaxAgeSeconds = 3600;

void removeStaleTempSymlinks(const std::filesystem::path& logDir)
{
    const std::time_t nowSeconds = std::time(nullptr);
    std::error_code errorCode;
    for (std::filesystem::directory_iterator it{logDir, errorCode}, end; it != end; it.increment(errorCode))
    {
        if (errorCode)
        {
            return;
        }
        const auto& entryPath = it->path();
        const auto name = entryPath.filename().string();
        if (not name.starts_with("latest.log.") or not name.ends_with(".tmp"))
        {
            continue;
        }
        /// lstat() reports the link's own mtime (not the target's), and works on a dangling link.
        struct stat linkStat{};
        if (::lstat(entryPath.c_str(), &linkStat) != 0 or not S_ISLNK(linkStat.st_mode))
        {
            continue;
        }
        /// A future-dated mtime (clock skew) yields a negative age and is left untouched.
        if (nowSeconds - linkStat.st_mtime > staleTempMaxAgeSeconds)
        {
            std::error_code removeErrorCode;
            std::filesystem::remove(entryPath, removeErrorCode);
        }
    }
}

void createSymlink(const std::filesystem::path& absoluteLogPath, const std::filesystem::path& symlinkPath)
{
    std::error_code errorCode;

    removeStaleTempSymlinks(symlinkPath.parent_path());

    const auto relativeLogPath = relative(absoluteLogPath, symlinkPath.parent_path(), errorCode);
    if (errorCode)
    {
        std::cerr << "Error calculating relative path during logger setup: " << errorCode.message() << "\n";
        return;
    }

    /// Two runner agents can share a work dir and race on the fixed `latest.log` path. A
    /// remove-then-create sequence is not atomic, so concurrent jobs fail with "File exists" (or
    /// "Permission denied" removing a symlink owned by the other agent). Create a unique temp
    /// symlink and rename() it over the target instead: rename is atomic and replaces the
    /// destination, so concurrent runs no longer collide. A random token makes the temp name
    /// unique: the PID alone is not, because agents in separate PID namespaces (containerized
    /// runners) can share a PID in the shared dir. The PID is kept only to identify which process
    /// left a stray temp behind.
    std::random_device randomDevice;
    const auto tempSymlinkPath = symlinkPath.parent_path() / fmt::format("latest.log.{:d}.{:08x}.tmp", ::getpid(), randomDevice());
    try
    {
        create_symlink(relativeLogPath, tempSymlinkPath);
    }
    catch (const std::filesystem::filesystem_error& e)
    {
        std::cerr << "Error creating symlink during logger setup: " << e.what() << '\n';
        return;
    }

    std::filesystem::rename(tempSymlinkPath, symlinkPath, errorCode);
    if (errorCode)
    {
        std::cerr << "Error installing symlink during logger setup: " << errorCode.message() << "\n";
        std::filesystem::remove(tempSymlinkPath, errorCode);
    }
}
}

void setupLogging(const SystestConfiguration& config)
{
    std::filesystem::path absoluteLogPath;
    const std::filesystem::path logDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests";

    if (config.logFilePath.getValue().empty())
    {
        std::error_code errorCode;
        create_directories(logDir, errorCode);
        if (errorCode)
        {
            std::cerr << "Error creating log directory during logger setup: " << errorCode.message() << "\n";
            return;
        }

        const auto now = std::chrono::system_clock::now();
        const auto pid = ::getpid();
        const std::string logFileName = fmt::format("SystemTest_{:%Y-%m-%d_%H-%M-%S}_{:d}.log", now, pid);

        absoluteLogPath = logDir / logFileName;
    }
    else
    {
        absoluteLogPath = config.logFilePath.getValue();
        const std::filesystem::path parentDir = absoluteLogPath.parent_path();
        if (not exists(parentDir) or not is_directory(parentDir))
        {
            fmt::println(std::cerr, "Error creating log file during logger setup: directory does not exist: file://{}", parentDir.string());
            std::exit(1); /// NOLINT(concurrency-mt-unsafe)
        }
    }

    fmt::println(std::cout, "Find the log at: file://{}", absoluteLogPath.string());
    Logger::setupLogging(absoluteLogPath.string(), LogLevel::LOG_DEBUG, false);

    const auto symlinkPath = logDir / "latest.log";
    createSymlink(absoluteLogPath, symlinkPath);
}

}
