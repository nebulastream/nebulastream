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

#include <SingleNodeWorker.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Configurations/ConfigValuePrinter.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayStorage.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/QueryPlanFingerprintUtil.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/QueryIdEncoding.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <cpptrace/from_current.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <NetworkOptions.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
extern void initNetworkServices(const std::string& connectionAddr, const NES::Host& host, const NES::NetworkOptions& options);
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{
constexpr std::string_view BundlePlanFileName = "plan.pb";
constexpr std::string_view BundleManifestFileName = "cache_manifest.json";
constexpr std::string_view BundleCacheDirectoryName = "cache";
constexpr std::string_view BundleStateDirectoryName = "state";
constexpr std::string_view BundleNamePrefix = "plan_";
constexpr std::string_view ReplayCheckpointBundleSuffix = "_replay_checkpoint_";
constexpr uint64_t CheckpointBundleFormatVersion = 1;

enum class CheckpointBundleKind : uint8_t
{
    Live = 0,
    ReplayCheckpoint = 1,
};

struct CheckpointBundlePaths
{
    std::string bundleName;
    std::filesystem::path rootDir;
    std::filesystem::path planFile;
    std::filesystem::path manifestFile;
    std::filesystem::path cacheDir;
    std::filesystem::path stateDir;
};

struct CheckpointCacheManifest
{
    QueryCompilation::CompilationCacheMode compilationCacheMode = QueryCompilation::CompilationCacheMode::Preferred;
    std::string cacheDirectory = std::string(BundleCacheDirectoryName);
    std::string planFingerprint;
    std::optional<std::string> replayRestoreFingerprint;
    CheckpointBundleKind bundleKind = CheckpointBundleKind::Live;
    std::string encodedQueryId;
    std::optional<uint64_t> checkpointWatermarkMs;
    std::vector<std::string> stateFiles;
};

struct RecoveredCheckpointBundle
{
    QueryId bundleQueryId = INVALID_QUERY_ID;
    std::filesystem::path recoveryBundleDirectory;
    std::filesystem::path recoveryPlanFile;
    CheckpointBundlePaths livePaths;
    CheckpointCacheManifest manifest;
};

struct CacheArtifactSnapshot
{
    std::vector<std::string> entries;
};

CheckpointBundlePaths createCheckpointBundlePaths(const std::filesystem::path& bundleDirectory)
{
    return {
        .bundleName = bundleDirectory.filename().string(),
        .rootDir = bundleDirectory,
        .planFile = bundleDirectory / BundlePlanFileName,
        .manifestFile = bundleDirectory / BundleManifestFileName,
        .cacheDir = bundleDirectory / BundleCacheDirectoryName,
        .stateDir = bundleDirectory / BundleStateDirectoryName};
}

CheckpointBundlePaths createCheckpointBundlePaths(const std::filesystem::path& checkpointDirectory, std::string_view bundleName)
{
    return createCheckpointBundlePaths(checkpointDirectory / std::string(bundleName));
}

std::string sanitizePlanFingerprintForPath(std::string_view planFingerprint)
{
    std::string sanitized;
    sanitized.reserve(planFingerprint.size());
    for (const auto ch : planFingerprint)
    {
        const auto isAsciiAlphaNumeric = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9');
        const auto isSafePunctuation = ch == '_' || ch == '-';
        sanitized.push_back(isAsciiAlphaNumeric || isSafePunctuation ? ch : '_');
    }
    if (sanitized.empty())
    {
        sanitized = "unknown";
    }
    return sanitized;
}

std::string makeCheckpointBundleName(const QueryId queryId, std::string_view planFingerprint)
{
    return fmt::format(
        "{}{}_{}",
        BundleNamePrefix,
        encodeQueryIdForIdentifier(queryId),
        sanitizePlanFingerprintForPath(planFingerprint));
}

std::string makeReplayCheckpointBundleName(const QueryId queryId, std::string_view planFingerprint, const uint64_t checkpointWatermarkMs)
{
    return fmt::format(
        "{}{}_{}{}{:020d}",
        BundleNamePrefix,
        encodeQueryIdForIdentifier(queryId),
        sanitizePlanFingerprintForPath(planFingerprint),
        ReplayCheckpointBundleSuffix,
        checkpointWatermarkMs);
}

std::optional<QueryId> parseBundleQueryId(std::string_view bundleName)
{
    if (!bundleName.starts_with(BundleNamePrefix))
    {
        return std::nullopt;
    }

    const auto suffix = bundleName.substr(BundleNamePrefix.size());
    const auto separator = suffix.find('_');
    if (separator == std::string_view::npos)
    {
        return std::nullopt;
    }

    const auto queryIdPart = suffix.substr(0, separator);
    return decodeQueryIdFromIdentifier(queryIdPart);
}

std::string checkpointBundleKindToString(const CheckpointBundleKind bundleKind)
{
    switch (bundleKind)
    {
        case CheckpointBundleKind::Live:
            return "live";
        case CheckpointBundleKind::ReplayCheckpoint:
            return "replay_checkpoint";
    }
    std::unreachable();
}

std::optional<CheckpointBundleKind> parseCheckpointBundleKind(std::string_view bundleKind)
{
    if (bundleKind == "live")
    {
        return CheckpointBundleKind::Live;
    }
    if (bundleKind == "replay_checkpoint")
    {
        return CheckpointBundleKind::ReplayCheckpoint;
    }
    return std::nullopt;
}

std::string compilationCacheModeToString(const QueryCompilation::CompilationCacheMode mode)
{
    switch (mode)
    {
        case QueryCompilation::CompilationCacheMode::Disabled:
            return "disabled";
        case QueryCompilation::CompilationCacheMode::Preferred:
            return "preferred";
        case QueryCompilation::CompilationCacheMode::Required:
            return "required";
    }
    std::unreachable();
}

std::optional<QueryCompilation::CompilationCacheMode> parseCompilationCacheMode(std::string_view mode)
{
    if (mode == "disabled")
    {
        return QueryCompilation::CompilationCacheMode::Disabled;
    }
    if (mode == "preferred")
    {
        return QueryCompilation::CompilationCacheMode::Preferred;
    }
    if (mode == "required")
    {
        return QueryCompilation::CompilationCacheMode::Required;
    }
    return std::nullopt;
}

std::string makeCheckpointRelativePath(const std::filesystem::path& absolutePath)
{
    const auto checkpointDirectory = CheckpointManager::getCheckpointDirectory();
    const auto relativePath = absolutePath.lexically_relative(checkpointDirectory);
    if (relativePath.empty() || relativePath.native().starts_with(".."))
    {
        throw CheckpointError(
            "Path {} is not inside checkpoint directory {}", absolutePath.string(), checkpointDirectory.string());
    }
    return relativePath.generic_string();
}

std::vector<std::string> enumerateStateFiles(const std::filesystem::path& stateDirectory)
{
    std::vector<std::string> stateFiles;
    std::error_code ec;
    for (std::filesystem::recursive_directory_iterator it(stateDirectory, ec); !ec && it != std::filesystem::recursive_directory_iterator(); ++it)
    {
        if (!it->is_regular_file())
        {
            continue;
        }
        const auto relativePath = it->path().lexically_relative(stateDirectory);
        if (relativePath.empty() || relativePath.native().starts_with(".."))
        {
            throw CheckpointError(
                "State file {} is not inside replay checkpoint state directory {}",
                it->path().string(),
                stateDirectory.string());
        }
        stateFiles.push_back(relativePath.generic_string());
    }
    if (ec)
    {
        throw CheckpointError("Cannot enumerate replay checkpoint state files in {}; {}", stateDirectory.string(), ec.message());
    }
    std::ranges::sort(stateFiles);
    return stateFiles;
}

std::string serializeCacheManifest(const CheckpointCacheManifest& manifest)
{
    auto manifestJson = nlohmann::json{
        {"format_version", CheckpointBundleFormatVersion},
        {"compilation_cache_mode", compilationCacheModeToString(manifest.compilationCacheMode)},
        {"compilation_cache_enabled", manifest.compilationCacheMode != QueryCompilation::CompilationCacheMode::Disabled},
        {"compilation_cache_directory", manifest.cacheDirectory},
        {"plan_fingerprint", manifest.planFingerprint},
        {"bundle_kind", checkpointBundleKindToString(manifest.bundleKind)},
        {"query_id", manifest.encodedQueryId},
        {"state_files", manifest.stateFiles}};
    if (manifest.replayRestoreFingerprint.has_value())
    {
        manifestJson["replay_restore_fingerprint"] = *manifest.replayRestoreFingerprint;
    }
    if (manifest.checkpointWatermarkMs.has_value())
    {
        manifestJson["checkpoint_watermark_ms"] = *manifest.checkpointWatermarkMs;
    }
    return manifestJson.dump(2);
}

std::optional<CheckpointCacheManifest> deserializeCacheManifest(const std::string& bytes)
{
    try
    {
        const auto manifestJson = nlohmann::json::parse(bytes);
        if (manifestJson.value("format_version", 0ULL) != CheckpointBundleFormatVersion)
        {
            return std::nullopt;
        }
        if (!manifestJson.contains("compilation_cache_mode") || !manifestJson["compilation_cache_mode"].is_string())
        {
            return std::nullopt;
        }
        if (!manifestJson.contains("compilation_cache_enabled") || !manifestJson["compilation_cache_enabled"].is_boolean())
        {
            return std::nullopt;
        }
        if (!manifestJson.contains("compilation_cache_directory") || !manifestJson["compilation_cache_directory"].is_string())
        {
            return std::nullopt;
        }
        if (!manifestJson.contains("plan_fingerprint") || !manifestJson["plan_fingerprint"].is_string())
        {
            return std::nullopt;
        }
        if (manifestJson.contains("replay_restore_fingerprint") && !manifestJson["replay_restore_fingerprint"].is_string())
        {
            return std::nullopt;
        }
        if (manifestJson.contains("bundle_kind") && !manifestJson["bundle_kind"].is_string())
        {
            return std::nullopt;
        }
        if (manifestJson.contains("query_id") && !manifestJson["query_id"].is_string())
        {
            return std::nullopt;
        }
        if (manifestJson.contains("state_files") && !manifestJson["state_files"].is_array())
        {
            return std::nullopt;
        }

        const auto bundleKindValue = manifestJson.value("bundle_kind", std::string("live"));
        const auto manifestMode = manifestJson["compilation_cache_mode"].get<std::string>();
        const auto cacheEnabled = manifestJson["compilation_cache_enabled"].get<bool>();
        auto parsedMode = parseCompilationCacheMode(manifestMode);
        if (!parsedMode)
        {
            return std::nullopt;
        }
        const auto parsedBundleKind = parseCheckpointBundleKind(bundleKindValue);
        if (!parsedBundleKind.has_value())
        {
            return std::nullopt;
        }
        if (!cacheEnabled)
        {
            parsedMode = QueryCompilation::CompilationCacheMode::Disabled;
        }
        return CheckpointCacheManifest{
            .compilationCacheMode = *parsedMode,
            .cacheDirectory = manifestJson["compilation_cache_directory"].get<std::string>(),
            .planFingerprint = manifestJson["plan_fingerprint"].get<std::string>(),
            .replayRestoreFingerprint = manifestJson.contains("replay_restore_fingerprint")
                    && manifestJson["replay_restore_fingerprint"].is_string()
                ? std::make_optional(manifestJson["replay_restore_fingerprint"].get<std::string>())
                : std::nullopt,
            .bundleKind = *parsedBundleKind,
            .encodedQueryId = manifestJson.value("query_id", std::string{}),
            .checkpointWatermarkMs = manifestJson.contains("checkpoint_watermark_ms")
                    && manifestJson["checkpoint_watermark_ms"].is_number_unsigned()
                ? std::make_optional(manifestJson["checkpoint_watermark_ms"].get<uint64_t>())
                : std::nullopt,
            .stateFiles = manifestJson.contains("state_files") && manifestJson["state_files"].is_array()
                ? manifestJson["state_files"].get<std::vector<std::string>>()
                : std::vector<std::string>{}};
    }
    catch (...)
    {
        return std::nullopt;
    }
}

std::optional<CheckpointCacheManifest> loadCheckpointManifest(const std::filesystem::path& bundleDirectory)
{
    const auto manifestBytes = CheckpointManager::loadFile(bundleDirectory / BundleManifestFileName);
    if (!manifestBytes.has_value())
    {
        return std::nullopt;
    }
    return deserializeCacheManifest(*manifestBytes);
}

CacheArtifactSnapshot captureCacheArtifactSnapshot(const std::filesystem::path& cacheDirectory)
{
    CacheArtifactSnapshot snapshot;
    std::error_code ec;
    if (cacheDirectory.empty() || !std::filesystem::exists(cacheDirectory, ec) || !std::filesystem::is_directory(cacheDirectory, ec))
    {
        return snapshot;
    }

    for (std::filesystem::recursive_directory_iterator it(cacheDirectory, ec); !ec && it != std::filesystem::recursive_directory_iterator(); ++it)
    {
        const auto& entry = *it;
        if (!entry.is_regular_file())
        {
            continue;
        }
        const auto relativePath = entry.path().lexically_relative(cacheDirectory).generic_string();
        const auto fileSize = entry.file_size();
        const auto lastWriteTime
            = std::chrono::duration_cast<std::chrono::nanoseconds>(entry.last_write_time().time_since_epoch()).count();
        snapshot.entries.push_back(fmt::format("{}|{}|{}", relativePath, fileSize, lastWriteTime));
    }
    if (ec)
    {
        NES_WARNING("Failed to inspect compilation cache directory {}: {}", cacheDirectory.string(), ec.message());
        snapshot.entries.clear();
        return snapshot;
    }
    std::ranges::sort(snapshot.entries);
    return snapshot;
}

std::string classifyRecoveryCacheOutcome(const CacheArtifactSnapshot& before, const CacheArtifactSnapshot& after)
{
    if (before.entries.empty() && after.entries.empty())
    {
        return "empty";
    }
    if (before.entries == after.entries)
    {
        return before.entries.empty() ? "empty" : "hot";
    }
    if (before.entries.empty())
    {
        return "cold_populated";
    }
    return "rewritten";
}

std::optional<QueryId> queryIdFromManifest(const CheckpointCacheManifest& manifest, std::string_view bundleName)
{
    if (!manifest.encodedQueryId.empty())
    {
        return decodeQueryIdFromIdentifier(manifest.encodedQueryId);
    }
    return parseBundleQueryId(bundleName);
}

std::optional<uint64_t> checkpointCoverageWatermarkMs(const std::vector<std::shared_ptr<OperatorHandler>>& statefulHandlers)
{
    std::optional<uint64_t> watermarkMs;
    for (const auto& handler : statefulHandlers)
    {
        if (!handler)
        {
            continue;
        }
        const auto handlerWatermark = handler->getCheckpointCoverageWatermark();
        if (!handlerWatermark.has_value())
        {
            continue;
        }
        const auto handlerWatermarkMs = handlerWatermark->getRawValue();
        watermarkMs = watermarkMs.has_value() ? std::min(*watermarkMs, handlerWatermarkMs) : handlerWatermarkMs;
    }
    return watermarkMs;
}

void replaceDirectoryAtomically(const std::filesystem::path& tempDirectory, const std::filesystem::path& targetDirectory)
{
    std::error_code ec;
    std::filesystem::remove_all(targetDirectory, ec);
    if (ec)
    {
        std::filesystem::remove_all(tempDirectory, ec);
        throw CheckpointError("Cannot replace checkpoint directory {}; {}", targetDirectory.string(), ec.message());
    }

    std::filesystem::rename(tempDirectory, targetDirectory, ec);
    if (ec)
    {
        std::filesystem::remove_all(tempDirectory, ec);
        throw CheckpointError("Cannot replace checkpoint directory {}; {}", targetDirectory.string(), ec.message());
    }
}

void persistReplayCheckpointBundle(
    const CheckpointBundlePaths& liveBundle,
    const CheckpointCacheManifest& liveManifest,
    const QueryId queryId,
    const std::vector<std::shared_ptr<OperatorHandler>>& statefulHandlers)
{
    const auto checkpointWatermarkMs = checkpointCoverageWatermarkMs(statefulHandlers);
    if (!checkpointWatermarkMs.has_value())
    {
        return;
    }

    auto replayManifest = liveManifest;
    replayManifest.bundleKind = CheckpointBundleKind::ReplayCheckpoint;
    replayManifest.encodedQueryId = encodeQueryIdForIdentifier(queryId);
    replayManifest.checkpointWatermarkMs = checkpointWatermarkMs;

    const auto replayBundle = createCheckpointBundlePaths(
        liveBundle.rootDir.parent_path(),
        makeReplayCheckpointBundleName(queryId, replayManifest.planFingerprint, *checkpointWatermarkMs));
    const auto tempReplayBundleDirectory = std::filesystem::path(replayBundle.rootDir.string() + ".tmp");

    std::error_code ec;
    std::filesystem::remove_all(tempReplayBundleDirectory, ec);
    if (ec)
    {
        throw CheckpointError(
            "Cannot prepare replay checkpoint bundle temp directory {}; {}",
            tempReplayBundleDirectory.string(),
            ec.message());
    }

    std::filesystem::copy(
        liveBundle.rootDir,
        tempReplayBundleDirectory,
        std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing,
        ec);
    if (ec)
    {
        std::filesystem::remove_all(tempReplayBundleDirectory, ec);
        throw CheckpointError("Cannot snapshot replay checkpoint bundle {}; {}", replayBundle.rootDir.string(), ec.message());
    }

    const auto tempReplayBundlePaths = createCheckpointBundlePaths(tempReplayBundleDirectory);
    replayManifest.stateFiles = enumerateStateFiles(tempReplayBundlePaths.stateDir);
    if (replayManifest.stateFiles.empty())
    {
        std::filesystem::remove_all(tempReplayBundleDirectory, ec);
        throw CheckpointError(
            "Replay checkpoint bundle {} does not contain any checkpoint state files",
            replayBundle.rootDir.string());
    }
    CheckpointManager::persistFile(
        makeCheckpointRelativePath(tempReplayBundlePaths.manifestFile), serializeCacheManifest(replayManifest));
    replaceDirectoryAtomically(tempReplayBundleDirectory, replayBundle.rootDir);
}

std::expected<void, Exception>
validateReplayCheckpointCompatibility(const std::filesystem::path& recoveryBundleDirectory, const CheckpointCacheManifest& manifest)
{
    if (!manifest.replayRestoreFingerprint.has_value() || manifest.replayRestoreFingerprint->empty())
    {
        return std::unexpected(QueryRegistrationFailed(
            "Replay checkpoint bundle {} is missing replay restore compatibility metadata",
            recoveryBundleDirectory.string()));
    }
    if (manifest.stateFiles.empty())
    {
        return std::unexpected(QueryRegistrationFailed(
            "Replay checkpoint bundle {} is missing an exact state file manifest for replay restore",
            recoveryBundleDirectory.string()));
    }

    const auto stateDirectory = createCheckpointBundlePaths(recoveryBundleDirectory).stateDir;
    std::error_code ec;
    if (!std::filesystem::exists(stateDirectory, ec) || !std::filesystem::is_directory(stateDirectory, ec))
    {
        return std::unexpected(QueryRegistrationFailed(
            "Replay checkpoint bundle {} is missing state directory {} required for replay restore",
            recoveryBundleDirectory.string(),
            stateDirectory.string()));
    }

    const auto actualStateFiles = enumerateStateFiles(stateDirectory);
    if (actualStateFiles != manifest.stateFiles)
    {
        return std::unexpected(QueryRegistrationFailed(
            "Replay checkpoint bundle {} state file manifest does not match the checkpoint state directory contents",
            recoveryBundleDirectory.string()));
    }
    return {};
}

std::vector<ReplayCheckpointStatus> findReplayCheckpointStatuses(const std::filesystem::path& checkpointDirectory)
{
    std::vector<ReplayCheckpointStatus> replayCheckpoints;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(checkpointDirectory, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
    {
        const auto& entry = *it;
        if (!entry.is_directory() || entry.path().filename() == ".recovery_snapshot")
        {
            continue;
        }

        const auto manifestBytes = CheckpointManager::loadFile(entry.path() / BundleManifestFileName);
        if (!manifestBytes.has_value())
        {
            continue;
        }

        const auto manifest = deserializeCacheManifest(*manifestBytes);
        if (!manifest.has_value() || manifest->bundleKind != CheckpointBundleKind::ReplayCheckpoint
            || !manifest->checkpointWatermarkMs.has_value())
        {
            continue;
        }

        const auto queryId = queryIdFromManifest(*manifest, entry.path().filename().string());
        if (!queryId.has_value())
        {
            continue;
        }

        replayCheckpoints.push_back(ReplayCheckpointStatus{
            .queryId = *queryId,
            .bundleName = entry.path().filename().string(),
            .planFingerprint = manifest->planFingerprint,
            .replayRestoreFingerprint = manifest->replayRestoreFingerprint.value_or(std::string{}),
            .checkpointWatermarkMs = *manifest->checkpointWatermarkMs});
    }

    if (ec)
    {
        NES_WARNING("Failed to inspect replay checkpoints in {}: {}", checkpointDirectory.string(), ec.message());
        return {};
    }

    std::ranges::sort(
        replayCheckpoints,
        [](const auto& lhs, const auto& rhs)
        {
            if (lhs.checkpointWatermarkMs != rhs.checkpointWatermarkMs)
            {
                return lhs.checkpointWatermarkMs < rhs.checkpointWatermarkMs;
            }
            return lhs.bundleName < rhs.bundleName;
        });
    return replayCheckpoints;
}

std::vector<RecoveredCheckpointBundle> findCheckpointBundles(const std::filesystem::path& checkpointDirectory)
{
    const auto recoveryDirectory = CheckpointManager::getCheckpointRecoveryDirectory();
    std::vector<RecoveredCheckpointBundle> recoveredBundles;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(recoveryDirectory, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
    {
        const auto& entry = *it;
        if (!entry.is_directory())
        {
            continue;
        }
        const auto planFile = entry.path() / BundlePlanFileName;
        if (!std::filesystem::is_regular_file(planFile, ec))
        {
            ec.clear();
            continue;
        }
        const auto recoveryBundleDirectory = entry.path();
        const auto manifestBytes = CheckpointManager::loadFile(recoveryBundleDirectory / BundleManifestFileName);
        if (!manifestBytes)
        {
            NES_WARNING("Checkpoint bundle {} is missing {}", recoveryBundleDirectory.string(), BundleManifestFileName);
            continue;
        }
        auto manifest = deserializeCacheManifest(*manifestBytes);
        if (!manifest)
        {
            NES_WARNING("Checkpoint bundle {} has an invalid cache manifest", recoveryBundleDirectory.string());
            continue;
        }
        if (manifest->bundleKind != CheckpointBundleKind::Live)
        {
            continue;
        }

        const auto bundleName = recoveryBundleDirectory.filename().string();
        const auto bundleQueryId = queryIdFromManifest(*manifest, bundleName);
        if (!bundleQueryId)
        {
            NES_WARNING("Checkpoint bundle {} has an invalid bundle name", recoveryBundleDirectory.string());
            continue;
        }
        auto livePaths = createCheckpointBundlePaths(checkpointDirectory, bundleName);
        livePaths.cacheDir = livePaths.rootDir / manifest->cacheDirectory;
        recoveredBundles.push_back(RecoveredCheckpointBundle{
            .bundleQueryId = *bundleQueryId,
            .recoveryBundleDirectory = recoveryBundleDirectory,
            .recoveryPlanFile = planFile,
            .livePaths = std::move(livePaths),
            .manifest = std::move(*manifest)});
    }
    if (ec)
    {
        NES_WARNING("Failed to inspect checkpoint bundles in {}: {}", recoveryDirectory.string(), ec.message());
        return {};
    }
    std::ranges::sort(
        recoveredBundles,
        [](const auto& lhs, const auto& rhs) { return lhs.livePaths.bundleName < rhs.livePaths.bundleName; });
    return recoveredBundles;
}

}

struct SingleNodeWorker::QueryRegistrationOptions
{
    std::optional<QueryId> recoveredQueryId;
    std::optional<std::filesystem::path> checkpointBundleDirectory;
    QueryCompilation::CompilationCacheMode compilationCacheMode = QueryCompilation::CompilationCacheMode::Preferred;
    std::optional<std::filesystem::path> compilationCacheDir;
    std::optional<std::filesystem::path> recoveryBundleDirectory;
    std::optional<CheckpointCacheManifest> recoveryBundleManifest;
    std::optional<std::string> recoveryPlanFingerprint;
    std::optional<std::string> replayRestoreFingerprint;
    std::optional<std::string> serializedPlanBytes;
    std::optional<ReplayQueryRuntimeControl> replayRuntimeControl;
    bool replayCheckpointRestore = false;
};

SingleNodeWorker::~SingleNodeWorker()
{
    CheckpointManager::shutdown();
}
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, Host host)
    : listener(std::make_shared<CompositeStatisticListener>())
    , replayStatisticsCollector(std::make_shared<ReplayStatisticsCollector>())
    , configuration(configuration)
{
    {
        std::stringstream configStr;
        ConfigValuePrinter printer(configStr);
        SingleNodeWorkerConfiguration(configuration).accept(printer);
        NES_INFO("Starting SingleNodeWorker {} with configuration:\n{}", host.getRawValue(), configStr.str());
    }
    const auto& checkpointConfig = configuration.workerConfiguration.checkpointConfiguration;
    const auto checkpointDir = std::filesystem::path(checkpointConfig.checkpointDirectory.getValue());
    const auto checkpointInterval = std::chrono::milliseconds(checkpointConfig.checkpointIntervalMs.getValue());
    const auto recoverFromCheckpoint = checkpointConfig.recoverFromCheckpoint.getValue();

    CheckpointManager::initialize(checkpointDir, checkpointInterval, recoverFromCheckpoint);
    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", host.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }
    listener->addQueryEngineListener(copyPtr(replayStatisticsCollector));

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(host);

    optimizer = std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryOptimization);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>(configuration.workerConfiguration.defaultQueryExecution);

    if (!configuration.data.getValue().empty())
    {
        const auto& networkConfig = configuration.workerConfiguration.network;
        initNetworkServices(
            configuration.data.getValue(),
            host,
            NetworkOptions{
                .senderQueueSize = static_cast<uint32_t>(networkConfig.senderQueueSize.getValue()),
                .maxPendingAcks = static_cast<uint32_t>(networkConfig.maxPendingAcks.getValue()),
                .receiverQueueSize = static_cast<uint32_t>(networkConfig.receiverQueueSize.getValue()),
                .senderIOThreads = static_cast<uint32_t>(networkConfig.senderIOThreads.getValue()),
                .receiverIOThreads = static_cast<uint32_t>(networkConfig.receiverIOThreads.getValue()),
            });
    }

    if (recoverFromCheckpoint)
    {
        recoverCheckpointBundles(checkpointDir);
    }
}

std::expected<QueryId, Exception>
SingleNodeWorker::registerQuery(
    LogicalPlan plan,
    std::optional<ReplayCheckpointRegistration> replayCheckpointRegistration,
    std::optional<ReplayQueryRuntimeControl> replayRuntimeControl) noexcept
{
    QueryRegistrationOptions options{};
    options.replayRuntimeControl = std::move(replayRuntimeControl);
    if (replayCheckpointRegistration.has_value())
    {
        options.replayRestoreFingerprint = replayCheckpointRegistration->replayRestoreFingerprint;
    }

    if (replayCheckpointRegistration.has_value() && replayCheckpointRegistration->restoreCheckpoint.has_value())
    {
        const auto& replayCheckpoint = *replayCheckpointRegistration->restoreCheckpoint;
        const auto recoveryBundleDirectory = CheckpointManager::getCheckpointDirectory() / replayCheckpoint.bundleName;
        std::error_code ec;
        const auto recoveryBundleExists = std::filesystem::exists(recoveryBundleDirectory, ec);
        const auto recoveryBundleIsDirectory = !ec && std::filesystem::is_directory(recoveryBundleDirectory, ec);
        if (ec || !recoveryBundleExists || !recoveryBundleIsDirectory)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Replay checkpoint bundle {} is not available in checkpoint directory {}",
                replayCheckpoint.bundleName,
                CheckpointManager::getCheckpointDirectory().string()));
        }

        const auto manifest = loadCheckpointManifest(recoveryBundleDirectory);
        if (!manifest.has_value() || manifest->bundleKind != CheckpointBundleKind::ReplayCheckpoint)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Replay checkpoint bundle {} is missing a valid replay checkpoint manifest",
                replayCheckpoint.bundleName));
        }
        if (manifest->planFingerprint != replayCheckpoint.planFingerprint)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Replay checkpoint bundle {} fingerprint mismatch: requested={} actual={}",
                replayCheckpoint.bundleName,
                replayCheckpoint.planFingerprint,
                manifest->planFingerprint));
        }
        const auto expectedReplayRestoreFingerprint
            = options.replayRestoreFingerprint.value_or(replayCheckpoint.replayRestoreFingerprint);
        if (!manifest->replayRestoreFingerprint.has_value()
            || manifest->replayRestoreFingerprint.value() != expectedReplayRestoreFingerprint)
        {
            return std::unexpected(QueryRegistrationFailed(
                "Replay checkpoint bundle {} replay restore fingerprint mismatch: requested={} actual={}",
                replayCheckpoint.bundleName,
                expectedReplayRestoreFingerprint,
                manifest->replayRestoreFingerprint.value_or(std::string("<missing>"))));
        }

        options.recoveryBundleDirectory = recoveryBundleDirectory;
        options.recoveryBundleManifest = *manifest;
        options.recoveryPlanFingerprint = manifest->planFingerprint;
        options.replayRestoreFingerprint = expectedReplayRestoreFingerprint;
        options.replayCheckpointRestore = true;
        return registerQueryInternal(std::move(plan), &options);
    }
    if (options.replayRuntimeControl.has_value() || options.replayRestoreFingerprint.has_value())
    {
        return registerQueryInternal(std::move(plan), &options);
    }
    return registerQueryInternal(std::move(plan), nullptr);
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQueryInternal(
    LogicalPlan plan, const QueryRegistrationOptions* options) noexcept
{
    CPPTRACE_TRY
    {
        QueryId assignedQueryId = plan.getQueryId();
        if (options != nullptr && options->recoveredQueryId.has_value())
        {
            assignedQueryId = options->recoveredQueryId.value();
        }
        else if (plan.getQueryId().getLocalQueryId() == INVALID_LOCAL_QUERY_ID)
        {
            auto localId = LocalQueryId(generateUUID());
            if (plan.getQueryId().isDistributed())
            {
                assignedQueryId = QueryId::create(localId, plan.getQueryId().getDistributedQueryId());
            }
            else
            {
                assignedQueryId = QueryId::createLocal(localId);
            }
        }
        plan.setQueryId(assignedQueryId);
        const LogContext context("queryId", plan.getQueryId());
        SerializableQueryPlan serializedPlanProto;
        std::string serializedPlan;
        if (options != nullptr && options->serializedPlanBytes.has_value())
        {
            serializedPlan = options->serializedPlanBytes.value();
            if (!serializedPlanProto.ParseFromString(serializedPlan))
            {
                throw CannotDeserialize("Failed to parse serialized checkpoint plan for recovered query registration");
            }
        }
        else
        {
            serializedPlanProto = QueryPlanSerializationUtil::serializeQueryPlan(plan);
            serializedPlanProto.SerializeToString(&serializedPlan);
        }
        if (!serializedPlanProto.has_queryid()
            || QueryPlanSerializationUtil::deserializeQueryId(serializedPlanProto.queryid()) != assignedQueryId)
        {
            *serializedPlanProto.mutable_queryid() = QueryPlanSerializationUtil::serializeQueryId(assignedQueryId);
            serializedPlanProto.SerializeToString(&serializedPlan);
        }
        const auto computedPlanFingerprint = computePlanFingerprint(serializedPlanProto);
        const auto recoveryPlanFingerprint
            = options != nullptr && options->recoveryPlanFingerprint.has_value()
            ? std::optional<std::string>{options->recoveryPlanFingerprint.value()}
            : std::nullopt;
        const auto bundlePlanFingerprint
            = options != nullptr && options->checkpointBundleDirectory.has_value() && recoveryPlanFingerprint.has_value()
            ? recoveryPlanFingerprint.value()
            : computedPlanFingerprint;

        if (recoveryPlanFingerprint.has_value() && recoveryPlanFingerprint.value() != computedPlanFingerprint
            && (options == nullptr || !options->replayCheckpointRestore))
        {
            NES_WARNING(
                "Recovered plan fingerprint mismatch: manifest={} computed={}",
                recoveryPlanFingerprint.value(),
                computedPlanFingerprint);
        }

        const auto checkpointIntervalMs = configuration.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.getValue();
        const auto checkpointingEnabled = checkpointIntervalMs > 0;
        const auto checkpointDirectory
            = std::filesystem::path(configuration.workerConfiguration.checkpointConfiguration.checkpointDirectory.getValue());

        std::optional<CheckpointBundlePaths> checkpointBundle;
        if (options != nullptr && options->checkpointBundleDirectory.has_value())
        {
            checkpointBundle = createCheckpointBundlePaths(options->checkpointBundleDirectory.value());
        }
        else if (checkpointingEnabled)
        {
            checkpointBundle = createCheckpointBundlePaths(
                checkpointDirectory, makeCheckpointBundleName(assignedQueryId, bundlePlanFingerprint));
        }

        const auto compilationCacheMode = options != nullptr ? options->compilationCacheMode
                                                             : (configuration.workerConfiguration.enableCompilationCache.getValue()
                                                                    ? QueryCompilation::CompilationCacheMode::Preferred
                                                                    : QueryCompilation::CompilationCacheMode::Disabled);
        std::optional<std::filesystem::path> compilationCacheDir;
        if (options != nullptr && options->compilationCacheDir.has_value())
        {
            compilationCacheDir = options->compilationCacheDir.value();
        }
        else if (checkpointBundle.has_value() && compilationCacheMode != QueryCompilation::CompilationCacheMode::Disabled)
        {
            compilationCacheDir = checkpointBundle->cacheDir;
        }
        else
        {
            const auto configuredCacheDir = configuration.workerConfiguration.compilationCacheDir.getValue();
            if (!configuredCacheDir.empty())
            {
                compilationCacheDir = std::filesystem::path(configuredCacheDir);
            }
        }

        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        const DumpMode dumpMode(
            configuration.workerConfiguration.dumpQueryCompilationIR.getValue(), configuration.workerConfiguration.dumpGraph.getValue());
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = dumpMode;
        request->compilationCacheMode = compilationCacheMode;
        request->compilationCacheDir = compilationCacheDir.has_value() ? compilationCacheDir->string() : std::string{};

        CacheArtifactSnapshot recoveryCacheBefore;
        const auto isRecoveryRegistration = options != nullptr && options->recoveryBundleDirectory.has_value();
        if (isRecoveryRegistration)
        {
            NES_INFO(
                "Recovering checkpoint bundle {} | cacheMode={} | cacheDir={} | planFingerprint={}",
                options->recoveryBundleDirectory->string(),
                compilationCacheModeToString(compilationCacheMode),
                compilationCacheDir.has_value() ? compilationCacheDir->string() : std::string("<none>"),
                recoveryPlanFingerprint.value_or(computedPlanFingerprint));
            if (compilationCacheMode != QueryCompilation::CompilationCacheMode::Disabled && compilationCacheDir.has_value())
            {
                recoveryCacheBefore = captureCacheArtifactSnapshot(compilationCacheDir.value());
            }
        }

        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        if (isRecoveryRegistration)
        {
            const auto recoveryCacheAfter
                = compilationCacheDir.has_value() ? captureCacheArtifactSnapshot(compilationCacheDir.value()) : CacheArtifactSnapshot{};
            NES_INFO(
                "Recovered checkpoint bundle {} compile result | cacheMode={} | cacheDir={} | cacheOutcome={} | artifactsBefore={} | artifactsAfter={}",
                options->recoveryBundleDirectory->string(),
                compilationCacheModeToString(compilationCacheMode),
                compilationCacheDir.has_value() ? compilationCacheDir->string() : std::string("<none>"),
                classifyRecoveryCacheOutcome(recoveryCacheBefore, recoveryCacheAfter),
                recoveryCacheBefore.entries.size(),
                recoveryCacheAfter.entries.size());
        }

        const auto checkpointStateDirectory
            = checkpointBundle.has_value() ? checkpointBundle->stateDir : CheckpointManager::getCheckpointDirectory();
        const auto checkpointRecoveryStateDirectory = options != nullptr && options->recoveryBundleDirectory.has_value()
            ? createCheckpointBundlePaths(options->recoveryBundleDirectory.value()).stateDir
            : CheckpointManager::getCheckpointRecoveryDirectory();
        const auto checkpointRecoveryEnabled
            = CheckpointManager::shouldRecoverFromCheckpoint()
            || (options != nullptr && options->recoveryBundleDirectory.has_value());
        auto statefulHandlers = result->statefulHandlers;
        for (const auto& handler : statefulHandlers)
        {
            if (handler)
            {
                handler->setCheckpointDirectories(
                    checkpointStateDirectory, checkpointRecoveryStateDirectory, checkpointRecoveryEnabled);
            }
        }
        if (options != nullptr && options->replayCheckpointRestore)
        {
            const auto compatibility = validateReplayCheckpointCompatibility(
                options->recoveryBundleDirectory.value(), options->recoveryBundleManifest.value());
            if (!compatibility)
            {
                return std::unexpected(compatibility.error());
            }
        }

        replayStatisticsCollector->registerCompiledQueryPlan(assignedQueryId, *result);
        if (options != nullptr && options->replayRuntimeControl.has_value())
        {
            replayStatisticsCollector->registerReplayQuery(assignedQueryId, *options->replayRuntimeControl);
        }
        nodeEngine->registerCompiledQueryPlan(assignedQueryId, std::move(result));
        const auto queryId = assignedQueryId;

        const auto callbackId = fmt::format("query_plan_{}", encodeQueryIdForIdentifier(queryId));
        const auto replayCheckpointCallbackId = fmt::format("query_replay_checkpoint_{}", encodeQueryIdForIdentifier(queryId));
        if (checkpointingEnabled && checkpointBundle.has_value())
        {
            auto manifest = CheckpointCacheManifest{
                .compilationCacheMode = compilationCacheMode,
                .cacheDirectory = checkpointBundle->cacheDir.lexically_relative(checkpointBundle->rootDir).empty()
                    ? std::string(BundleCacheDirectoryName)
                    : checkpointBundle->cacheDir.lexically_relative(checkpointBundle->rootDir).generic_string(),
                .planFingerprint = bundlePlanFingerprint,
                .replayRestoreFingerprint = options != nullptr ? options->replayRestoreFingerprint : std::nullopt,
                .bundleKind = CheckpointBundleKind::Live,
                .encodedQueryId = encodeQueryIdForIdentifier(queryId),
                .checkpointWatermarkMs = std::nullopt,
                .stateFiles = {}};
            auto persistPlan = [planBytes = serializedPlan,
                                manifestBytes = serializeCacheManifest(manifest),
                                bundle = *checkpointBundle]() mutable
            {
                CheckpointManager::persistFile(makeCheckpointRelativePath(bundle.planFile), planBytes);
                CheckpointManager::persistFile(makeCheckpointRelativePath(bundle.manifestFile), manifestBytes);
            };
            persistPlan();
            CheckpointManager::registerCallback(
                callbackId, std::move(persistPlan), CheckpointManager::CallbackPhase::Prepare);
            if (!statefulHandlers.empty())
            {
                auto persistReplayCheckpoint = [bundle = *checkpointBundle,
                                                manifest,
                                                queryId,
                                                statefulHandlers]() mutable
                {
                    persistReplayCheckpointBundle(bundle, manifest, queryId, statefulHandlers);
                };
                CheckpointManager::registerCallback(
                    replayCheckpointCallbackId,
                    std::move(persistReplayCheckpoint),
                    CheckpointManager::CallbackPhase::Commit);
            }
        }
        return queryId;
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->startQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(QueryId queryId, QueryTerminationType type) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId, type);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::unregisterQuery(QueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_QUERY_ID, "QueryId must be not invalid!");
        replayStatisticsCollector->unregisterQuery(queryId);
        nodeEngine->unregisterQuery(queryId);
        CheckpointManager::unregisterCallback(fmt::format("query_plan_{}", encodeQueryIdForIdentifier(queryId)));
        CheckpointManager::unregisterCallback(fmt::format("query_state_{}", encodeQueryIdForIdentifier(queryId)));
        CheckpointManager::unregisterCallback(fmt::format("query_replay_checkpoint_{}", encodeQueryIdForIdentifier(queryId)));
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<LocalQueryStatus, Exception> SingleNodeWorker::getQueryStatus(QueryId queryId) const noexcept
{
    CPPTRACE_TRY
    {
        auto status = nodeEngine->getQueryLog()->getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return status.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<std::vector<uint64_t>, Exception>
SingleNodeWorker::selectReplaySegments(const std::string& recordingFilePath, const Replay::BinaryStoreReplaySelection& selection) const noexcept
{
    CPPTRACE_TRY
    {
        auto selectedSegments = Replay::selectBinaryStoreSegments(recordingFilePath, selection);
        std::vector<uint64_t> segmentIds;
        segmentIds.reserve(selectedSegments.size());
        for (const auto& segment : selectedSegments)
        {
            segmentIds.push_back(segment.segmentId);
        }
        return segmentIds;
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<std::vector<uint64_t>, Exception>
SingleNodeWorker::pinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) noexcept
{
    CPPTRACE_TRY
    {
        return Replay::pinBinaryStoreSegments(recordingFilePath, segmentIds);
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception>
SingleNodeWorker::unpinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) noexcept
{
    CPPTRACE_TRY
    {
        Replay::unpinBinaryStoreSegments(recordingFilePath, segmentIds);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    const std::chrono::system_clock::time_point until = std::chrono::system_clock::now();
    const auto summaries = nodeEngine->getQueryLog()->getStatus();
    WorkerStatus status;
    status.after = after;
    status.until = until;
    for (const auto& [queryId, state, metrics] : summaries)
    {
        switch (state)
        {
            case QueryState::Registered:
                /// Ignore these for the worker status
                break;
            case QueryState::Started:
                ++status.replayMetrics.activeQueryCount;
                INVARIANT(metrics.start.has_value(), "If query is started, it should have a start timestamp");
                if (metrics.start.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, std::nullopt);
                }
                break;
            case QueryState::Running: {
                ++status.replayMetrics.activeQueryCount;
                INVARIANT(metrics.running.has_value(), "If query is running, it should have a running timestamp");
                if (metrics.running.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, metrics.running.value());
                }
                break;
            }
            case QueryState::Stopped: {
                INVARIANT(metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
                INVARIANT(metrics.stop.has_value(), "If query is stopped, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
            case QueryState::Failed: {
                INVARIANT(metrics.stop.has_value(), "If query has failed, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
        }
    }
    status.replayMetrics.recordingStatuses = Replay::getRecordingRuntimeStatuses();
    for (const auto& recordingStatus : status.replayMetrics.recordingStatuses)
    {
        status.replayMetrics.recordingStorageBytes += recordingStatus.storageBytes;
    }
    status.replayMetrics.recordingFileCount = status.replayMetrics.recordingStatuses.size();
    status.replayMetrics.replayReadBytes = Replay::getReplayReadBytes();
    status.replayMetrics.operatorStatistics = replayStatisticsCollector->snapshot();
    status.replayMetrics.replayQueryStatuses = replayStatisticsCollector->snapshotReplayQueries();
    status.replayMetrics.replayCheckpoints = findReplayCheckpointStatuses(CheckpointManager::getCheckpointDirectory());
    return status;
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(QueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

void SingleNodeWorker::recoverCheckpointBundles(const std::filesystem::path& checkpointDir)
{
    try
    {
        if (!std::filesystem::exists(checkpointDir))
        {
            NES_INFO("Checkpoint directory {} does not exist, skipping recovery", checkpointDir.string());
            return;
        }
        if (!std::filesystem::is_directory(checkpointDir))
        {
            NES_WARNING("Checkpoint path {} is not a directory, skipping recovery", checkpointDir.string());
            return;
        }

        const auto recoveredBundles = findCheckpointBundles(checkpointDir);
        if (recoveredBundles.empty())
        {
            NES_INFO("No checkpoint bundles found in {}", CheckpointManager::getCheckpointRecoveryDirectory().string());
            return;
        }

        for (const auto& recoveredBundle : recoveredBundles)
        {
            auto serializedPlanOpt = loadSerializedPlanFromFile(recoveredBundle.recoveryPlanFile);
            if (!serializedPlanOpt)
            {
                NES_WARNING(
                    "Checkpoint bundle {} does not contain a valid serialized plan",
                    recoveredBundle.recoveryPlanFile.string());
                continue;
            }

            SerializableQueryPlan proto;
            if (!proto.ParseFromString(*serializedPlanOpt))
            {
                NES_WARNING("Failed to parse serialized plan from {}", recoveredBundle.recoveryPlanFile.string());
                continue;
            }

            auto plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
            QueryRegistrationOptions options{
                .recoveredQueryId = plan.getQueryId() != INVALID_QUERY_ID ? std::optional<QueryId>{plan.getQueryId()}
                                                                          : std::optional<QueryId>{recoveredBundle.bundleQueryId},
                .checkpointBundleDirectory = recoveredBundle.livePaths.rootDir,
                .compilationCacheMode = recoveredBundle.manifest.compilationCacheMode,
                .compilationCacheDir = recoveredBundle.livePaths.cacheDir,
                .recoveryBundleDirectory = recoveredBundle.recoveryBundleDirectory,
                .recoveryBundleManifest = recoveredBundle.manifest,
                .recoveryPlanFingerprint = recoveredBundle.manifest.planFingerprint,
                .replayRestoreFingerprint = recoveredBundle.manifest.replayRestoreFingerprint,
                .serializedPlanBytes = *serializedPlanOpt,
                .replayRuntimeControl = std::nullopt};
            auto registerResult = registerQueryInternal(plan, &options);
            if (!registerResult)
            {
                NES_WARNING(
                    "Failed to register recovered query from checkpoint bundle {}: {}",
                    recoveredBundle.livePaths.rootDir.string(),
                    registerResult.error().what());
                continue;
            }
            NES_INFO(
                "Recovered query {} from checkpoint bundle {}",
                registerResult.value(),
                recoveredBundle.livePaths.rootDir.string());
            if (auto startResult = startQuery(registerResult.value()); !startResult)
            {
                NES_WARNING("Failed to start recovered query {}: {}", registerResult.value(), startResult.error().what());
            }
        }
    }
    catch (const Exception& ex)
    {
        NES_WARNING("Exception during checkpoint recovery: {}", ex.what());
    }
    catch (const std::exception& stdex)
    {
        NES_WARNING("Std exception during checkpoint recovery: {}", stdex.what());
    }
}

std::optional<std::string> SingleNodeWorker::loadSerializedPlanFromFile(const std::filesystem::path& planFilePath)
{
    return CheckpointManager::loadFile(planFilePath);
}

}
