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
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <fmt/format.h>
#include <cpptrace/from_current.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
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
constexpr uint64_t CheckpointBundleFormatVersion = 1;

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
    return fmt::format("{}{}_{}", BundleNamePrefix, queryId.getRawValue(), sanitizePlanFingerprintForPath(planFingerprint));
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

    QueryId::Underlying queryIdValue = 0;
    const auto queryIdPart = suffix.substr(0, separator);
    const auto [ptr, errorCode] = std::from_chars(queryIdPart.data(), queryIdPart.data() + queryIdPart.size(), queryIdValue);
    if (errorCode != std::errc{} || ptr != queryIdPart.data() + queryIdPart.size())
    {
        return std::nullopt;
    }
    return QueryId(queryIdValue);
}

std::string computeFingerprintFromBytes(const std::string_view bytes)
{
    constexpr uint64_t fnvOffsetBasis = 1469598103934665603ULL;
    constexpr uint64_t fnvPrime = 1099511628211ULL;

    uint64_t hash = fnvOffsetBasis;
    for (const auto byte : bytes)
    {
        hash ^= static_cast<uint8_t>(byte);
        hash *= fnvPrime;
    }
    return fmt::format("fnv1a64:{:016x}", hash);
}

template <typename ProtoMessage>
std::string serializeProtoDeterministically(const ProtoMessage& message)
{
    std::string bytes;
    bytes.reserve(static_cast<size_t>(message.ByteSizeLong()));
    google::protobuf::io::StringOutputStream rawOutput(&bytes);
    google::protobuf::io::CodedOutputStream codedOutput(&rawOutput);
    codedOutput.SetSerializationDeterministic(true);
    if (!message.SerializeToCodedStream(&codedOutput))
    {
        throw CannotSerialize("Failed to deterministically serialize canonical query plan for fingerprinting");
    }
    return bytes;
}

SerializableQueryPlan canonicalizePlanForFingerprint(const SerializableQueryPlan& serializedPlan)
{
    std::unordered_map<OperatorId::Underlying, const SerializableOperator*> operatorsById;
    operatorsById.reserve(serializedPlan.operators_size());
    for (const auto& op : serializedPlan.operators())
    {
        operatorsById.emplace(op.operator_id(), &op);
    }

    std::unordered_map<OperatorId::Underlying, OperatorId::Underlying> canonicalIds;
    canonicalIds.reserve(operatorsById.size());
    OperatorId::Underlying nextOperatorId = INITIAL_OPERATOR_ID.getRawValue();
    std::vector<OperatorId::Underlying> worklist;
    worklist.reserve(serializedPlan.rootoperatorids_size() + serializedPlan.operators_size());
    for (const auto rootOperatorId : serializedPlan.rootoperatorids())
    {
        worklist.push_back(rootOperatorId);
    }

    for (size_t idx = 0; idx < worklist.size(); ++idx)
    {
        const auto originalOperatorId = worklist[idx];
        if (canonicalIds.contains(originalOperatorId))
        {
            continue;
        }
        canonicalIds.emplace(originalOperatorId, nextOperatorId++);
        if (const auto opIt = operatorsById.find(originalOperatorId); opIt != operatorsById.end())
        {
            for (const auto childId : opIt->second->children_ids())
            {
                worklist.push_back(childId);
            }
        }
    }

    std::vector<OperatorId::Underlying> remainingOperatorIds;
    remainingOperatorIds.reserve(operatorsById.size());
    for (const auto& [operatorId, _] : operatorsById)
    {
        if (!canonicalIds.contains(operatorId))
        {
            remainingOperatorIds.push_back(operatorId);
        }
    }
    std::ranges::sort(remainingOperatorIds);
    for (const auto operatorId : remainingOperatorIds)
    {
        canonicalIds.emplace(operatorId, nextOperatorId++);
    }

    std::vector<SerializableOperator> canonicalOperators;
    canonicalOperators.reserve(serializedPlan.operators_size());
    for (const auto& op : serializedPlan.operators())
    {
        auto canonicalOperator = op;
        canonicalOperator.set_operator_id(canonicalIds.at(op.operator_id()));
        for (int childIdx = 0; childIdx < canonicalOperator.children_ids_size(); ++childIdx)
        {
            canonicalOperator.set_children_ids(childIdx, canonicalIds.at(op.children_ids(childIdx)));
        }
        canonicalOperators.push_back(std::move(canonicalOperator));
    }
    std::ranges::sort(
        canonicalOperators,
        [](const auto& lhs, const auto& rhs) { return lhs.operator_id() < rhs.operator_id(); });

    auto canonicalPlan = serializedPlan;
    canonicalPlan.clear_rootoperatorids();
    canonicalPlan.clear_operators();
    for (const auto rootOperatorId : serializedPlan.rootoperatorids())
    {
        canonicalPlan.add_rootoperatorids(canonicalIds.at(rootOperatorId));
    }
    for (const auto& op : canonicalOperators)
    {
        *canonicalPlan.add_operators() = op;
    }
    return canonicalPlan;
}

std::string computePlanFingerprint(const SerializableQueryPlan& serializedPlan)
{
    auto canonicalPlan = canonicalizePlanForFingerprint(serializedPlan);
    canonicalPlan.clear_queryid();
    auto canonicalPlanBytes = serializeProtoDeterministically(canonicalPlan);
    return computeFingerprintFromBytes(canonicalPlanBytes);
}

QueryId reserveRecoveredQueryId(const QueryId recoveredQueryId, std::atomic<QueryId::Underlying>& queryIdCounter)
{
    const auto nextQueryId = recoveredQueryId.getRawValue() + 1;
    auto current = queryIdCounter.load(std::memory_order_relaxed);
    while (current < nextQueryId
           && !queryIdCounter.compare_exchange_weak(current, nextQueryId, std::memory_order_relaxed, std::memory_order_relaxed))
    {
    }
    return recoveredQueryId;
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

std::string serializeCacheManifest(const CheckpointCacheManifest& manifest)
{
    const nlohmann::json manifestJson = {
        {"format_version", CheckpointBundleFormatVersion},
        {"compilation_cache_mode", compilationCacheModeToString(manifest.compilationCacheMode)},
        {"compilation_cache_enabled", manifest.compilationCacheMode != QueryCompilation::CompilationCacheMode::Disabled},
        {"compilation_cache_directory", manifest.cacheDirectory},
        {"plan_fingerprint", manifest.planFingerprint}};
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

        const auto manifestMode = manifestJson["compilation_cache_mode"].get<std::string>();
        const auto cacheEnabled = manifestJson["compilation_cache_enabled"].get<bool>();
        auto parsedMode = parseCompilationCacheMode(manifestMode);
        if (!parsedMode)
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
            .planFingerprint = manifestJson["plan_fingerprint"].get<std::string>()};
    }
    catch (...)
    {
        return std::nullopt;
    }
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

        const auto bundleName = recoveryBundleDirectory.filename().string();
        const auto bundleQueryId = parseBundleQueryId(bundleName);
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
    std::optional<std::string> recoveryPlanFingerprint;
    std::optional<std::string> serializedPlanBytes;
};

SingleNodeWorker::~SingleNodeWorker()
{
    CheckpointManager::shutdown();
}
SingleNodeWorker::SingleNodeWorker(SingleNodeWorker&& other) noexcept = default;
SingleNodeWorker& SingleNodeWorker::operator=(SingleNodeWorker&& other) noexcept = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& configuration, WorkerId workerId)
    : listener(std::make_shared<CompositeStatisticListener>()), configuration(configuration)
{
    const auto& checkpointConfig = configuration.workerConfiguration.checkpointConfiguration;
    const auto checkpointDir = std::filesystem::path(checkpointConfig.checkpointDirectory.getValue());
    const auto checkpointInterval = std::chrono::milliseconds(checkpointConfig.checkpointIntervalMs.getValue());
    const auto recoverFromCheckpoint = checkpointConfig.recoverFromCheckpoint.getValue();

    CheckpointManager::initialize(checkpointDir, checkpointInterval, recoverFromCheckpoint);

    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json", workerId.getRawValue(), std::chrono::system_clock::now(), ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(workerId);

    optimizer = std::make_unique<QueryOptimizer>(configuration.workerConfiguration.defaultQueryExecution);
    compiler = std::make_unique<QueryCompilation::QueryCompiler>();

    if (recoverFromCheckpoint)
    {
        recoverCheckpointBundles(checkpointDir);
    }
}

/// This is a workaround to get again unique queryId after our initial worker refactoring.
/// We might want to move this to the engine.
static std::atomic queryIdCounter = INITIAL<QueryId>.getRawValue();

std::expected<QueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    return registerQueryInternal(std::move(plan), nullptr);
}

std::expected<QueryId, Exception> SingleNodeWorker::registerQueryInternal(
    LogicalPlan plan, const QueryRegistrationOptions* options) noexcept
{
    CPPTRACE_TRY
    {
        const auto assignedQueryId = options != nullptr && options->recoveredQueryId.has_value()
            ? reserveRecoveredQueryId(options->recoveredQueryId.value(), queryIdCounter)
            : QueryId(queryIdCounter.fetch_add(1, std::memory_order_relaxed));
        plan.setQueryId(assignedQueryId);
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
        if (!serializedPlanProto.has_queryid() || QueryId(serializedPlanProto.queryid()) != assignedQueryId)
        {
            serializedPlanProto.set_queryid(assignedQueryId.getRawValue());
            serializedPlanProto.SerializeToString(&serializedPlan);
        }
        const auto computedPlanFingerprint = computePlanFingerprint(serializedPlanProto);
        const auto planFingerprint = options != nullptr && options->recoveryPlanFingerprint.has_value()
            ? options->recoveryPlanFingerprint.value()
            : computedPlanFingerprint;

        if (options != nullptr && options->recoveryPlanFingerprint.has_value()
            && options->recoveryPlanFingerprint.value() != computedPlanFingerprint)
        {
            NES_WARNING(
                "Recovered plan fingerprint mismatch: manifest={} computed={}",
                options->recoveryPlanFingerprint.value(),
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
            checkpointBundle = createCheckpointBundlePaths(checkpointDirectory, makeCheckpointBundleName(assignedQueryId, planFingerprint));
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
        listener->onEvent(SubmitQuerySystemEvent{queryPlan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
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
                planFingerprint);
            if (compilationCacheMode != QueryCompilation::CompilationCacheMode::Disabled && compilationCacheDir.has_value())
            {
                recoveryCacheBefore = captureCacheArtifactSnapshot(compilationCacheDir.value());
            }
        }

        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successfull query compilation or exception, but got nothing");
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
        auto statefulHandlers = result->statefulHandlers;
        for (const auto& handler : statefulHandlers)
        {
            if (handler)
            {
                handler->setCheckpointDirectories(checkpointStateDirectory, checkpointRecoveryStateDirectory);
            }
        }

        const auto queryId = nodeEngine->registerCompiledQueryPlan(std::move(result));
        if (queryId != assignedQueryId)
        {
            NES_WARNING("Assigned query id {} differs from registered query id {}", assignedQueryId, queryId);
        }

        if (!statefulHandlers.empty())
        {
            const auto callbackId = fmt::format("query_state_{}", queryId.getRawValue());
            CheckpointManager::registerCallback(
                callbackId,
                [handlers = std::move(statefulHandlers), checkpointStateDirectory]
                {
                    for (const auto& handler : handlers)
                    {
                        if (handler)
                        {
                            handler->serializeState(checkpointStateDirectory);
                        }
                    }
                },
                CheckpointManager::CallbackPhase::Prepare);
        }

        const auto callbackId = fmt::format("query_plan_{}", queryId.getRawValue());
        if (checkpointingEnabled && checkpointBundle.has_value())
        {
            auto manifest = CheckpointCacheManifest{
                .compilationCacheMode = compilationCacheMode,
                .cacheDirectory = checkpointBundle->cacheDir.lexically_relative(checkpointBundle->rootDir).empty()
                    ? std::string(BundleCacheDirectoryName)
                    : checkpointBundle->cacheDir.lexically_relative(checkpointBundle->rootDir).generic_string(),
                .planFingerprint = planFingerprint};
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
        nodeEngine->unregisterQuery(queryId);
        CheckpointManager::unregisterCallback(fmt::format("query_plan_{}", queryId.getRawValue()));
        CheckpointManager::unregisterCallback(fmt::format("query_state_{}", queryId.getRawValue()));
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
                .recoveryPlanFingerprint = recoveredBundle.manifest.planFingerprint,
                .serializedPlanBytes = *serializedPlanOpt};
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
