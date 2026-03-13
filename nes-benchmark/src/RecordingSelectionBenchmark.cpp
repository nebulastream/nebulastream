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

#include <LegacyOptimizer/RecordingBoundarySolver.hpp>
#include <LegacyOptimizer/RecordingSelectionTypes.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <RecordingSelectionResult.hpp>
#include <WorkerCatalog.hpp>
#include <nlohmann/json.hpp>

namespace NES
{
namespace
{

using Json = nlohmann::json;

constexpr double EPSILON = 1e-9;
constexpr size_t DEFAULT_TREE_HEIGHT = 2;
constexpr size_t DEFAULT_MIN_FANOUT = 2;
constexpr size_t DEFAULT_MAX_FANOUT = 16;
constexpr uint64_t DEFAULT_SEED_COUNT = 16;
constexpr size_t DEFAULT_SOLVER_REPETITIONS = 64;
constexpr size_t DEFAULT_HOST_COUNT = 3;
constexpr double DEFAULT_LATENCY_QUANTILE = 0.65;
constexpr double DEFAULT_STORAGE_QUANTILE = 0.70;
constexpr size_t DEFAULT_SHADOW_PRICE_ITERATIONS = 32;
constexpr size_t CORPUS_SCHEMA_VERSION = 3;
constexpr size_t REPORT_SCHEMA_VERSION = 1;

enum class DagScenarioKind : uint8_t;

enum class ScenarioFamily
{
    Tree,
    DagMix
};

enum class CorpusTier
{
    Exact,
    Mixed,
    PerformanceOnly
};

enum class BaselineMode
{
    Exact,
    PerformanceOnly
};

enum class NetworkTopology
{
    Line,
    Star,
    Tree,
    Ring,
    RandomConnected
};

enum class DagScenarioKind : uint8_t
{
    Diamond,
    Join,
    Pipeline,
    Union,
    MultiJoin,
    JoinUnionHybrid,
    SharedSubplan,
    BranchingPipeline,
    ReconvergingFanout,
    MergedPipeline,
    MergedUnion,
    MergedJoin
};

struct BenchmarkConfig
{
    ScenarioFamily scenarioFamily = ScenarioFamily::Tree;
    CorpusTier corpusTier = CorpusTier::Exact;
    NetworkTopology networkTopology = NetworkTopology::Line;
    std::vector<DagScenarioKind> dagFamilies;
    std::vector<double> dagFamilyWeights;
    size_t treeHeight = DEFAULT_TREE_HEIGHT;
    size_t minFanout = DEFAULT_MIN_FANOUT;
    size_t maxFanout = DEFAULT_MAX_FANOUT;
    uint64_t firstSeed = 0;
    uint64_t seedCount = DEFAULT_SEED_COUNT;
    size_t solverRepetitions = DEFAULT_SOLVER_REPETITIONS;
    size_t hostCount = DEFAULT_HOST_COUNT;
    double latencyQuantile = DEFAULT_LATENCY_QUANTILE;
    double storageQuantile = DEFAULT_STORAGE_QUANTILE;
    bool shadowPriceSweep = false;
    bool shadowPriceConvergence = false;
    bool includeNonbindingSweepScenarios = false;
    std::vector<double> replayInitialPriceScales = {0.25, 0.5, 1.0, 2.0, 4.0};
    std::vector<double> replayStepScales = {0.25, 0.5, 1.0, 2.0, 4.0};
    double storageStepScale = 1.0;
    size_t shadowPriceIterations = DEFAULT_SHADOW_PRICE_ITERATIONS;
    bool legacyAdaptive = false;
    bool corpusOnly = false;
    std::optional<std::string> outputPath;
    std::optional<std::string> reportOutputPath;
    std::optional<std::string> corpusInputPath;
    std::optional<std::string> corpusOutputPath;
};

struct RecordingPlanEdgeHash
{
    [[nodiscard]] size_t operator()(const RecordingPlanEdge& edge) const
    {
        return std::hash<uint64_t>{}(edge.parentId.getRawValue()) ^ (std::hash<uint64_t>{}(edge.childId.getRawValue()) << 1U);
    }
};

struct TreeNode
{
    OperatorId id{INVALID_OPERATOR_ID};
    std::string kind;
    std::vector<size_t> childEdgeIndices;
    double replayTimeMs = 0.0;
    bool isLeaf = false;
};

struct TreeEdge
{
    RecordingPlanEdge edge;
    size_t parentNodeIndex = 0;
    size_t childNodeIndex = 0;
    size_t hostIndex = 0;
    std::vector<Host> routeNodes;
    std::optional<RecordingBoundaryCandidate> candidate;
};

struct CutMetrics
{
    double maintenanceCost = 0.0;
    double replayTimeMs = 0.0;
    std::vector<size_t> storageBytesByHost;
    size_t selectedEdgeCount = 0;
};

struct ScenarioStats
{
    size_t validCutCount = 0;
    size_t feasibleCutCount = 0;
    double bestMaintenanceCost = 0.0;
    double bestReplayTimeMs = 0.0;
    double averageMaintenanceCost = 0.0;
    double worstMaintenanceCost = 0.0;
};

struct CandidateOptionSummary
{
    size_t candidateEdges = 0;
    size_t candidateOptions = 0;
    size_t reuseOptions = 0;
    size_t upgradeOptions = 0;
    size_t createOptions = 0;
};

struct ScenarioRow
{
    std::string scenario;
    std::string baselineMode = "exact";
    std::string networkTopology = "line";
    uint64_t seed = 0;
    size_t treeHeight = 0;
    size_t fanout = 0;
    size_t hostCount = 0;
    size_t nodeCount = 0;
    size_t candidateEdges = 0;
    size_t candidateOptions = 0;
    size_t reuseOptions = 0;
    size_t upgradeOptions = 0;
    size_t createOptions = 0;
    size_t leafCount = 0;
    size_t validCutCount = 0;
    size_t feasibleCutCount = 0;
    uint64_t replayLatencyLimitMs = 0;
    size_t selectedEdgeCount = 0;
    double selectedMaintenanceCost = 0.0;
    double selectedReplayTimeMs = 0.0;
    double bestMaintenanceCost = 0.0;
    double bestReplayTimeMs = 0.0;
    double averageMaintenanceCost = 0.0;
    double worstMaintenanceCost = 0.0;
    double selectedOverBest = 0.0;
    double averageOverBest = 0.0;
    double worstOverBest = 0.0;
    size_t selectedRank = 0;
    double solverTimeNsMean = 0.0;
    std::string benchmarkMode;
    std::string instanceId;
    std::string corpusBucket;
    std::string budgetProfile;
    double feasibleFraction = 0.0;
    double latencyQuantile = 0.0;
    double storageQuantile = 0.0;
    double unconstrainedBestMaintenanceCost = 0.0;
    double unconstrainedBestReplayTimeMs = 0.0;
    bool constraintsBind = false;
    double replayInitialPriceScale = 1.0;
    double replayStepScale = 1.0;
    double storageStepScale = 1.0;
    size_t shadowPriceIterations = DEFAULT_SHADOW_PRICE_ITERATIONS;
    bool solverSucceeded = true;
    bool optimalHit = false;
};

struct ScenarioData
{
    std::string scenarioName = "kary-tree-recording-selection";
    BaselineMode baselineMode = BaselineMode::Exact;
    NetworkTopology networkTopology = NetworkTopology::Line;
    RecordingCandidateSet candidateSet;
    std::vector<TreeNode> nodes;
    std::vector<TreeEdge> edges;
    size_t rootNodeIndex = 0;
    std::vector<Host> hosts;
    std::vector<std::vector<size_t>> hostNeighborsByIndex;
    std::vector<size_t> storageBudgetsBytes;
    uint64_t replayLatencyLimitMs = 0;
    bool usesGenericExactEnumeration = false;
};

[[nodiscard]] const RecordingBoundaryCandidate& requireCandidate(const TreeEdge& edge)
{
    if (!edge.candidate.has_value())
    {
        throw std::runtime_error("synthetic scenario edge does not expose recording candidates");
    }
    return *edge.candidate;
}

[[nodiscard]] bool edgeHasCandidate(const TreeEdge& edge) { return edge.candidate.has_value(); }

struct BudgetAttempt
{
    std::string profileName;
    double latencyQuantile = 0.0;
    double storageQuantile = 0.0;
};

struct PerformanceBudgetAttempt
{
    std::string bucketName;
    std::string profileName;
    double latencyQuantile = 0.0;
    double storageQuantile = 0.0;
};

struct PerformanceReferenceSet
{
    CutMetrics unconstrainedReference;
    CutMetrics replayTightReference;
    std::vector<size_t> maxStorageByHost;
};

struct CorpusBucket
{
    std::string name;
    double minFeasibleFraction = 0.0;
    double maxFeasibleFraction = 0.0;
    std::vector<BudgetAttempt> attempts;
};

struct FrozenScenario
{
    std::string scenario;
    std::string baselineMode = "exact";
    std::string networkTopology = "line";
    std::string instanceId;
    std::string corpusBucket;
    std::string budgetProfile;
    uint64_t seed = 0;
    size_t treeHeight = 0;
    size_t fanout = 0;
    size_t hostCount = 0;
    size_t nodeCount = 0;
    size_t candidateEdges = 0;
    size_t candidateOptions = 0;
    size_t reuseOptions = 0;
    size_t upgradeOptions = 0;
    size_t createOptions = 0;
    double latencyQuantile = 0.0;
    double storageQuantile = 0.0;
    uint64_t replayLatencyLimitMs = 0;
    std::vector<size_t> storageBudgetsBytes;
    size_t validCutCount = 0;
    size_t feasibleCutCount = 0;
    double feasibleFraction = 0.0;
    double bestMaintenanceCost = 0.0;
    double bestReplayTimeMs = 0.0;
    double averageMaintenanceCost = 0.0;
    double worstMaintenanceCost = 0.0;
    double unconstrainedBestMaintenanceCost = 0.0;
    double unconstrainedBestReplayTimeMs = 0.0;
    bool constraintsBind = false;
};

struct BaseScenarioCacheEntry
{
    ScenarioData baseScenario;
    std::vector<CutMetrics> allCuts;
    CutMetrics unconstrainedBest;
};

struct SolverInvocationResult
{
    std::optional<RecordingBoundarySelection> selection;
    double elapsedNs = 0.0;
    bool succeeded = false;
};

struct SelectionWithMetrics
{
    RecordingBoundarySelection selection;
    CutMetrics metrics;
};

struct ConvergenceTraceRow
{
    std::string scenario;
    std::string baselineMode = "exact";
    std::string networkTopology = "line";
    std::string instanceId;
    std::string corpusBucket;
    std::string budgetProfile;
    uint64_t seed = 0;
    size_t treeHeight = 0;
    size_t fanout = 0;
    size_t hostCount = 0;
    size_t nodeCount = 0;
    size_t candidateEdges = 0;
    size_t candidateOptions = 0;
    size_t iteration = 0;
    bool constraintsBind = false;
    double feasibleFraction = 0.0;
    double replayInitialPriceScale = 1.0;
    double replayStepScale = 1.0;
    double storageStepScale = 1.0;
    size_t shadowPriceIterations = DEFAULT_SHADOW_PRICE_ITERATIONS;
    bool solverSucceeded = false;
    double replayTimePrice = 0.0;
    double selectedReplayTimeMs = 0.0;
    double replayConstraintSatisfactionPercent = 100.0;
    double storageBudgetConstraintSatisfactionPercent = 100.0;
    double maxStorageUtilizationPercent = 0.0;
    bool replayConstraintSatisfied = true;
    bool storageBudgetConstraintSatisfied = true;
    bool currentIterateFeasible = true;
    bool returnableSolutionFeasible = false;
};

[[nodiscard]] uint64_t powU64(const uint64_t base, const size_t exponent)
{
    uint64_t result = 1;
    for (size_t index = 0; index < exponent; ++index)
    {
        result *= base;
    }
    return result;
}

[[nodiscard]] double positiveRatio(const double numerator, const double denominator)
{
    return denominator <= EPSILON ? 0.0 : numerator / denominator;
}

[[nodiscard]] bool almostEqual(const double lhs, const double rhs)
{
    return std::abs(lhs - rhs) <= EPSILON;
}

[[nodiscard]] double parseDouble(const std::string_view input)
{
    size_t parsedCharacters = 0;
    const auto value = std::stod(std::string(input), &parsedCharacters);
    if (parsedCharacters != input.size())
    {
        throw std::invalid_argument("unable to parse floating point value");
    }
    return value;
}

[[nodiscard]] std::vector<double> parseDoubleList(const std::string_view input)
{
    std::vector<double> values;
    size_t offset = 0;
    while (offset < input.size())
    {
        const auto separator = input.find(',', offset);
        const auto token = input.substr(offset, separator == std::string_view::npos ? input.size() - offset : separator - offset);
        if (token.empty())
        {
            throw std::invalid_argument("unable to parse floating point list: empty entry");
        }
        values.push_back(parseDouble(token));
        if (separator == std::string_view::npos)
        {
            break;
        }
        offset = separator + 1;
    }
    if (values.empty())
    {
        throw std::invalid_argument("unable to parse floating point list: no entries");
    }
    return values;
}

[[nodiscard]] std::vector<std::string> parseStringList(const std::string_view input)
{
    std::vector<std::string> values;
    size_t offset = 0;
    while (offset < input.size())
    {
        const auto separator = input.find(',', offset);
        const auto token = input.substr(offset, separator == std::string_view::npos ? input.size() - offset : separator - offset);
        if (token.empty())
        {
            throw std::invalid_argument("unable to parse string list: empty entry");
        }
        values.emplace_back(token);
        if (separator == std::string_view::npos)
        {
            break;
        }
        offset = separator + 1;
    }
    if (values.empty())
    {
        throw std::invalid_argument("unable to parse string list: no entries");
    }
    return values;
}

[[nodiscard]] ScenarioFamily parseScenarioFamily(const std::string_view input)
{
    if (input == "tree")
    {
        return ScenarioFamily::Tree;
    }
    if (input == "dag-mix")
    {
        return ScenarioFamily::DagMix;
    }
    throw std::invalid_argument("unknown scenario family: " + std::string(input));
}

[[nodiscard]] CorpusTier parseCorpusTier(const std::string_view input)
{
    if (input == "exact")
    {
        return CorpusTier::Exact;
    }
    if (input == "mixed")
    {
        return CorpusTier::Mixed;
    }
    if (input == "performance-only")
    {
        return CorpusTier::PerformanceOnly;
    }
    throw std::invalid_argument("unknown corpus tier: " + std::string(input));
}

[[nodiscard]] std::string_view baselineModeName(const BaselineMode baselineMode)
{
    switch (baselineMode)
    {
        case BaselineMode::Exact:
            return "exact";
        case BaselineMode::PerformanceOnly:
            return "performance-only";
    }

    throw std::invalid_argument("unsupported baseline mode");
}

[[nodiscard]] BaselineMode parseBaselineMode(const std::string_view input)
{
    if (input == "exact")
    {
        return BaselineMode::Exact;
    }
    if (input == "performance-only")
    {
        return BaselineMode::PerformanceOnly;
    }
    throw std::invalid_argument("unknown baseline mode: " + std::string(input));
}

[[nodiscard]] bool hasExactBaseline(const BaselineMode baselineMode)
{
    return baselineMode == BaselineMode::Exact;
}

[[nodiscard]] std::string_view networkTopologyName(const NetworkTopology networkTopology)
{
    switch (networkTopology)
    {
        case NetworkTopology::Line:
            return "line";
        case NetworkTopology::Star:
            return "star";
        case NetworkTopology::Tree:
            return "tree";
        case NetworkTopology::Ring:
            return "ring";
        case NetworkTopology::RandomConnected:
            return "random-connected";
    }

    throw std::invalid_argument("unsupported network topology");
}

[[nodiscard]] NetworkTopology parseNetworkTopology(const std::string_view input)
{
    if (input == "line")
    {
        return NetworkTopology::Line;
    }
    if (input == "star")
    {
        return NetworkTopology::Star;
    }
    if (input == "tree")
    {
        return NetworkTopology::Tree;
    }
    if (input == "ring")
    {
        return NetworkTopology::Ring;
    }
    if (input == "random-connected")
    {
        return NetworkTopology::RandomConnected;
    }
    throw std::invalid_argument("unknown network topology: " + std::string(input));
}

[[nodiscard]] std::string_view recordingDecisionName(const RecordingSelectionDecision decision)
{
    switch (decision)
    {
        case RecordingSelectionDecision::CreateNewRecording:
            return "create";
        case RecordingSelectionDecision::UpgradeExistingRecording:
            return "upgrade";
        case RecordingSelectionDecision::ReuseExistingRecording:
            return "reuse";
    }

    throw std::invalid_argument("unsupported recording selection decision");
}

[[nodiscard]] std::string_view recordingRepresentationName(const RecordingRepresentation representation)
{
    switch (representation)
    {
        case RecordingRepresentation::BinaryStore:
            return "BinaryStore";
        case RecordingRepresentation::BinaryStoreZstdFast1:
            return "BinaryStoreZstdFast1";
        case RecordingRepresentation::BinaryStoreZstd:
            return "BinaryStoreZstd";
        case RecordingRepresentation::BinaryStoreZstdFast6:
            return "BinaryStoreZstdFast6";
    }

    throw std::invalid_argument("unsupported recording representation");
}

[[nodiscard]] DagScenarioKind parseDagScenarioKind(const std::string_view input);
[[nodiscard]] std::vector<DagScenarioKind> parseDagScenarioKinds(const std::string_view input);

[[nodiscard]] uint64_t parseUnsigned(const std::string_view input)
{
    size_t parsedCharacters = 0;
    const auto value = std::stoull(std::string(input), &parsedCharacters);
    if (parsedCharacters != input.size())
    {
        throw std::invalid_argument("unable to parse unsigned integer value");
    }
    return value;
}

void printUsage(std::ostream& out)
{
    out << "Usage: recording-selection-benchmark [options]\n"
           "Default mode benchmarks the solver on a deterministic frozen corpus whose budgets\n"
           "are chosen offline from fixed quantile profiles.\n"
           "\n"
           "  --scenario-family <tree|dag-mix>  Scenario generator used for the benchmark corpus (default: tree)\n"
           "  --corpus-tier <exact|mixed|performance-only>  Corpus tier for dag-mix scenarios (default: exact)\n"
           "  --dag-families <csv>       Comma-separated dag-mix families to sample from\n"
           "  --dag-family-weights <csv> Optional weights aligned with --dag-families\n"
           "  --network-topology <line|star|tree|ring|random-connected>  Host network used for dag-mix routes (default: line)\n"
           "  --height <n>               Tree height used for the exact baseline (default: 2)\n"
           "  --min-fanout <n>           Smallest tree fanout or DAG size parameter to evaluate (default: 2)\n"
           "  --max-fanout <n>           Largest tree fanout or DAG size parameter to evaluate (default: 16)\n"
           "  --first-seed <n>           First deterministic seed (default: 0)\n"
           "  --seed-count <n>           Number of deterministic instances to evaluate per size (default: 16)\n"
           "  --solver-repetitions <n>   Number of solver timings per scenario (default: 64)\n"
           "  --hosts <n>                Worker count used for storage budgets or DAG host upper bound (default: 3)\n"
           "  --latency-quantile <q>     Starting replay-latency quantile for legacy adaptive mode\n"
           "  --storage-quantile <q>     Starting storage quantile for legacy adaptive mode\n"
           "  --shadow-price-sweep       Sweep replay shadow-price scales over the frozen corpus\n"
           "  --shadow-price-convergence Emit per-iteration replay/storage constraint-satisfaction traces over the frozen corpus\n"
           "  --include-nonbinding-scenarios  Keep non-binding frozen scenarios in shadow-price modes\n"
           "  --replay-initial-price-scales <csv>  Comma-separated replay warm-start scales (default: 0.25,0.5,1,2,4)\n"
           "  --replay-step-scales <csv> Comma-separated replay step scales (default: 0.25,0.5,1,2,4)\n"
           "  --storage-step-scale <x>   Storage shadow-price step scale used in sweep mode (default: 1)\n"
           "  --shadow-price-iterations <n>  Max shadow-price iterations used in sweep mode (default: 32)\n"
           "  --legacy-adaptive          Run the old benchmark that relaxes budgets until all cuts are feasible\n"
           "  --corpus-input <path>      Read a frozen corpus JSON file instead of generating one\n"
           "  --corpus-output <path>     Write the generated frozen corpus JSON file\n"
           "  --report-output <path>     Write visualization report JSON for the frozen corpus\n"
           "  --corpus-only              Generate or rewrite the corpus and exit without benchmarking\n"
           "  --output <path>            Write CSV to a file instead of stdout\n"
           "  --help                     Show this message\n";
}

[[nodiscard]] BenchmarkConfig parseArguments(const int argc, char** argv)
{
    BenchmarkConfig config;
    for (int index = 1; index < argc; ++index)
    {
        const std::string_view argument(argv[index]);
        const auto requireValue = [&](const std::string_view option) -> std::string_view
        {
            if (index + 1 >= argc)
            {
                throw std::invalid_argument("missing value for " + std::string(option));
            }
            ++index;
            return argv[index];
        };

        if (argument == "--help")
        {
            printUsage(std::cout);
            std::exit(0);
        }
        if (argument == "--scenario-family")
        {
            config.scenarioFamily = parseScenarioFamily(requireValue(argument));
            continue;
        }
        if (argument == "--corpus-tier")
        {
            config.corpusTier = parseCorpusTier(requireValue(argument));
            continue;
        }
        if (argument == "--dag-families")
        {
            config.dagFamilies = parseDagScenarioKinds(requireValue(argument));
            continue;
        }
        if (argument == "--dag-family-weights")
        {
            config.dagFamilyWeights = parseDoubleList(requireValue(argument));
            continue;
        }
        if (argument == "--network-topology")
        {
            config.networkTopology = parseNetworkTopology(requireValue(argument));
            continue;
        }
        if (argument == "--height")
        {
            config.treeHeight = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--min-fanout")
        {
            config.minFanout = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--max-fanout")
        {
            config.maxFanout = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--first-seed")
        {
            config.firstSeed = parseUnsigned(requireValue(argument));
            continue;
        }
        if (argument == "--seed-count")
        {
            config.seedCount = parseUnsigned(requireValue(argument));
            continue;
        }
        if (argument == "--solver-repetitions")
        {
            config.solverRepetitions = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--hosts")
        {
            config.hostCount = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--latency-quantile")
        {
            config.latencyQuantile = parseDouble(requireValue(argument));
            continue;
        }
        if (argument == "--storage-quantile")
        {
            config.storageQuantile = parseDouble(requireValue(argument));
            continue;
        }
        if (argument == "--shadow-price-sweep")
        {
            config.shadowPriceSweep = true;
            continue;
        }
        if (argument == "--shadow-price-convergence")
        {
            config.shadowPriceConvergence = true;
            continue;
        }
        if (argument == "--include-nonbinding-scenarios")
        {
            config.includeNonbindingSweepScenarios = true;
            continue;
        }
        if (argument == "--replay-initial-price-scales")
        {
            config.replayInitialPriceScales = parseDoubleList(requireValue(argument));
            continue;
        }
        if (argument == "--replay-step-scales")
        {
            config.replayStepScales = parseDoubleList(requireValue(argument));
            continue;
        }
        if (argument == "--storage-step-scale")
        {
            config.storageStepScale = parseDouble(requireValue(argument));
            continue;
        }
        if (argument == "--shadow-price-iterations")
        {
            config.shadowPriceIterations = static_cast<size_t>(parseUnsigned(requireValue(argument)));
            continue;
        }
        if (argument == "--legacy-adaptive")
        {
            config.legacyAdaptive = true;
            continue;
        }
        if (argument == "--corpus-input")
        {
            config.corpusInputPath = std::string(requireValue(argument));
            continue;
        }
        if (argument == "--corpus-output")
        {
            config.corpusOutputPath = std::string(requireValue(argument));
            continue;
        }
        if (argument == "--report-output")
        {
            config.reportOutputPath = std::string(requireValue(argument));
            continue;
        }
        if (argument == "--corpus-only")
        {
            config.corpusOnly = true;
            continue;
        }
        if (argument == "--output")
        {
            config.outputPath = std::string(requireValue(argument));
            continue;
        }
        throw std::invalid_argument("unknown argument: " + std::string(argument));
    }

    if (config.treeHeight == 0)
    {
        throw std::invalid_argument("--height must be at least 1");
    }
    if (config.minFanout < 2 || config.maxFanout < config.minFanout)
    {
        throw std::invalid_argument("fanout range must satisfy 2 <= min-fanout <= max-fanout");
    }
    if (config.seedCount == 0)
    {
        throw std::invalid_argument("--seed-count must be at least 1");
    }
    if (config.solverRepetitions == 0)
    {
        throw std::invalid_argument("--solver-repetitions must be at least 1");
    }
    if (config.hostCount == 0)
    {
        throw std::invalid_argument("--hosts must be at least 1");
    }
    if (config.latencyQuantile < 0.0 || config.latencyQuantile > 1.0 || config.storageQuantile < 0.0 || config.storageQuantile > 1.0)
    {
        throw std::invalid_argument("quantiles must be in the inclusive range [0, 1]");
    }
    if (config.shadowPriceIterations == 0)
    {
        throw std::invalid_argument("--shadow-price-iterations must be at least 1");
    }
    const auto validateNonNegativeScales = [](const std::vector<double>& scales, const std::string_view option) -> void
    {
        if (std::ranges::any_of(scales, [](const double value) { return value < 0.0; }))
        {
            throw std::invalid_argument(std::string(option) + " values must be non-negative");
        }
    };
    validateNonNegativeScales(config.replayInitialPriceScales, "--replay-initial-price-scales");
    validateNonNegativeScales(config.replayStepScales, "--replay-step-scales");
    if (std::ranges::any_of(config.dagFamilyWeights, [](const double value) { return value < 0.0; }))
    {
        throw std::invalid_argument("--dag-family-weights values must be non-negative");
    }
    if (config.storageStepScale < 0.0)
    {
        throw std::invalid_argument("--storage-step-scale must be non-negative");
    }
    if (!config.dagFamilyWeights.empty() && !config.dagFamilies.empty() && config.dagFamilyWeights.size() != config.dagFamilies.size())
    {
        throw std::invalid_argument("--dag-family-weights must have the same length as --dag-families");
    }
    if (config.scenarioFamily == ScenarioFamily::Tree && config.corpusTier != CorpusTier::Exact)
    {
        throw std::invalid_argument("--corpus-tier supports non-exact modes only with --scenario-family dag-mix");
    }
    if (config.scenarioFamily == ScenarioFamily::Tree
        && (!config.dagFamilies.empty() || !config.dagFamilyWeights.empty() || config.networkTopology != NetworkTopology::Line))
    {
        throw std::invalid_argument("--dag-families, --dag-family-weights, and non-line --network-topology require --scenario-family dag-mix");
    }
    if (config.legacyAdaptive && (config.corpusInputPath.has_value() || config.corpusOutputPath.has_value() || config.corpusOnly))
    {
        throw std::invalid_argument("--legacy-adaptive cannot be combined with corpus options");
    }
    if (config.legacyAdaptive && config.corpusTier != CorpusTier::Exact)
    {
        throw std::invalid_argument("--legacy-adaptive requires --corpus-tier exact");
    }
    if (config.legacyAdaptive && config.shadowPriceSweep)
    {
        throw std::invalid_argument("--legacy-adaptive cannot be combined with --shadow-price-sweep");
    }
    if (config.legacyAdaptive && config.shadowPriceConvergence)
    {
        throw std::invalid_argument("--legacy-adaptive cannot be combined with --shadow-price-convergence");
    }
    if (config.shadowPriceSweep && config.shadowPriceConvergence)
    {
        throw std::invalid_argument("--shadow-price-sweep cannot be combined with --shadow-price-convergence");
    }
    if (config.corpusOnly && config.corpusInputPath.has_value())
    {
        throw std::invalid_argument("--corpus-only cannot be combined with --corpus-input");
    }
    if (config.corpusOnly && !config.corpusOutputPath.has_value())
    {
        throw std::invalid_argument("--corpus-only requires --corpus-output");
    }
    if (config.corpusOnly && config.reportOutputPath.has_value())
    {
        throw std::invalid_argument("--report-output cannot be combined with --corpus-only");
    }
    if (config.shadowPriceSweep && config.corpusOnly)
    {
        throw std::invalid_argument("--shadow-price-sweep cannot be combined with --corpus-only");
    }
    if (config.shadowPriceConvergence && config.corpusOnly)
    {
        throw std::invalid_argument("--shadow-price-convergence cannot be combined with --corpus-only");
    }
    if (config.shadowPriceSweep && config.reportOutputPath.has_value())
    {
        throw std::invalid_argument("--report-output cannot be combined with --shadow-price-sweep");
    }
    if (config.shadowPriceConvergence && config.reportOutputPath.has_value())
    {
        throw std::invalid_argument("--report-output cannot be combined with --shadow-price-convergence");
    }
    if (config.legacyAdaptive && config.reportOutputPath.has_value())
    {
        throw std::invalid_argument("--report-output cannot be combined with --legacy-adaptive");
    }
    const std::vector<double> defaultSweepScales = {0.25, 0.5, 1.0, 2.0, 4.0};
    if (config.shadowPriceConvergence && config.replayInitialPriceScales == defaultSweepScales && config.replayStepScales == defaultSweepScales)
    {
        config.replayInitialPriceScales = {1.0};
        config.replayStepScales = {1.0};
    }
    if (config.shadowPriceConvergence && config.replayInitialPriceScales.size() != 1)
    {
        throw std::invalid_argument("--shadow-price-convergence requires exactly one replay initial price scale");
    }
    if (config.shadowPriceConvergence && config.replayStepScales.size() != 1)
    {
        throw std::invalid_argument("--shadow-price-convergence requires exactly one replay step scale");
    }
    return config;
}

[[nodiscard]] RecordingCandidateOption makeCandidateOption(
    const Host& host,
    const size_t subtreeLeafCount,
    const double maintenanceCost,
    const size_t storageBytes,
    const uint64_t replayLatencyMs,
    const uint64_t recordingNumber)
{
    return RecordingCandidateOption{
        .decision = RecordingSelectionDecision::CreateNewRecording,
        .selection = RecordingSelection{
            .recordingId = RecordingId("synthetic-recording-" + std::to_string(recordingNumber)),
            .node = host,
            .filePath = {},
            .structuralFingerprint = {},
            .representation = RecordingRepresentation::BinaryStore,
            .retentionWindowMs = std::nullopt,
            .beneficiaryQueries = {},
            .coversIncomingQuery = true},
        .cost = RecordingCostBreakdown{
            .decisionCost = maintenanceCost,
            .estimatedStorageBytes = storageBytes,
            .incrementalStorageBytes = storageBytes,
            .operatorCount = subtreeLeafCount,
            .estimatedReplayBandwidthBytesPerSecond = 0,
            .estimatedReplayLatencyMs = replayLatencyMs,
            .maintenanceCost = maintenanceCost,
            .replayCost = 0.0,
            .recomputeCost = 0.0,
            .replayTimeMultiplier = 1.0,
            .boundaryCutCost = maintenanceCost,
            .replayRecomputeCost = 0.0,
            .fitsBudget = true,
            .satisfiesReplayLatency = true},
        .feasible = true,
        .infeasibilityReason = {}};
}

[[nodiscard]] double weightedReplayScanTimeMs(const RecordingCandidateOption& option)
{
    return static_cast<double>(option.cost.estimatedReplayLatencyMs) * option.cost.replayTimeMultiplier;
}

[[nodiscard]] size_t findHostIndex(const std::vector<Host>& hosts, const Host& host)
{
    const auto it = std::ranges::find(hosts, host);
    if (it == hosts.end())
    {
        throw std::runtime_error("scenario references an unknown host");
    }
    return static_cast<size_t>(std::distance(hosts.begin(), it));
}

[[nodiscard]] std::vector<Host> makeBenchmarkHosts(const size_t hostCount);

void connectHostIndices(std::vector<std::vector<size_t>>& hostNeighborsByIndex, const size_t lhs, const size_t rhs)
{
    if (lhs == rhs || lhs >= hostNeighborsByIndex.size() || rhs >= hostNeighborsByIndex.size())
    {
        return;
    }
    hostNeighborsByIndex[lhs].push_back(rhs);
    hostNeighborsByIndex[rhs].push_back(lhs);
}

void normalizeHostNeighbors(std::vector<std::vector<size_t>>& hostNeighborsByIndex)
{
    for (auto& neighbors : hostNeighborsByIndex)
    {
        std::ranges::sort(neighbors);
        neighbors.erase(std::unique(neighbors.begin(), neighbors.end()), neighbors.end());
    }
}

void initializeScenarioNetwork(ScenarioData& scenario, const size_t hostCount, const NetworkTopology networkTopology, const uint64_t seed)
{
    scenario.networkTopology = networkTopology;
    scenario.hosts = makeBenchmarkHosts(hostCount);
    scenario.hostNeighborsByIndex.assign(hostCount, {});
    if (hostCount <= 1)
    {
        return;
    }

    switch (networkTopology)
    {
        case NetworkTopology::Line:
        {
            for (size_t hostIndex = 1; hostIndex < hostCount; ++hostIndex)
            {
                connectHostIndices(scenario.hostNeighborsByIndex, hostIndex - 1, hostIndex);
            }
            break;
        }
        case NetworkTopology::Star:
        {
            for (size_t hostIndex = 1; hostIndex < hostCount; ++hostIndex)
            {
                connectHostIndices(scenario.hostNeighborsByIndex, 0, hostIndex);
            }
            break;
        }
        case NetworkTopology::Tree:
        {
            for (size_t hostIndex = 1; hostIndex < hostCount; ++hostIndex)
            {
                connectHostIndices(scenario.hostNeighborsByIndex, (hostIndex - 1) / 2, hostIndex);
            }
            break;
        }
        case NetworkTopology::Ring:
        {
            for (size_t hostIndex = 1; hostIndex < hostCount; ++hostIndex)
            {
                connectHostIndices(scenario.hostNeighborsByIndex, hostIndex - 1, hostIndex);
            }
            if (hostCount > 2)
            {
                connectHostIndices(scenario.hostNeighborsByIndex, 0, hostCount - 1);
            }
            break;
        }
        case NetworkTopology::RandomConnected:
        {
            std::mt19937_64 randomEngine(seed ^ (static_cast<uint64_t>(hostCount) << 32U) ^ 0x9e3779b97f4a7c15ULL);
            for (size_t hostIndex = 1; hostIndex < hostCount; ++hostIndex)
            {
                std::uniform_int_distribution<size_t> existingHost(0, hostIndex - 1);
                connectHostIndices(scenario.hostNeighborsByIndex, hostIndex, existingHost(randomEngine));
            }
            const auto extraEdgeBudget = std::max<size_t>(1, hostCount / 2);
            std::uniform_int_distribution<size_t> hostDistribution(0, hostCount - 1);
            for (size_t edgeIndex = 0; edgeIndex < extraEdgeBudget; ++edgeIndex)
            {
                auto lhs = hostDistribution(randomEngine);
                auto rhs = hostDistribution(randomEngine);
                if (lhs == rhs)
                {
                    rhs = (rhs + 1) % hostCount;
                }
                connectHostIndices(scenario.hostNeighborsByIndex, lhs, rhs);
            }
            break;
        }
    }

    normalizeHostNeighbors(scenario.hostNeighborsByIndex);
}

[[nodiscard]] std::vector<Host> routeBetweenHostIndices(const ScenarioData& scenario, size_t fromIndex, size_t toIndex)
{
    if (scenario.hosts.empty())
    {
        throw std::runtime_error("route construction requires at least one host");
    }

    fromIndex = std::min(fromIndex, scenario.hosts.size() - 1);
    toIndex = std::min(toIndex, scenario.hosts.size() - 1);
    if (fromIndex == toIndex)
    {
        return {scenario.hosts[fromIndex]};
    }

    if (scenario.hostNeighborsByIndex.size() != scenario.hosts.size())
    {
        throw std::runtime_error("scenario network graph is not initialized");
    }

    std::vector<std::optional<size_t>> predecessorByIndex(scenario.hosts.size(), std::nullopt);
    std::vector<size_t> queue;
    queue.reserve(scenario.hosts.size());
    queue.push_back(fromIndex);
    predecessorByIndex[fromIndex] = fromIndex;

    for (size_t queueOffset = 0; queueOffset < queue.size(); ++queueOffset)
    {
        const auto currentIndex = queue[queueOffset];
        for (const auto neighborIndex : scenario.hostNeighborsByIndex[currentIndex])
        {
            if (predecessorByIndex[neighborIndex].has_value())
            {
                continue;
            }
            predecessorByIndex[neighborIndex] = currentIndex;
            if (neighborIndex == toIndex)
            {
                break;
            }
            queue.push_back(neighborIndex);
        }
        if (predecessorByIndex[toIndex].has_value())
        {
            break;
        }
    }

    if (!predecessorByIndex[toIndex].has_value())
    {
        throw std::runtime_error("scenario network graph is disconnected");
    }

    std::vector<Host> route;
    for (auto currentIndex = toIndex;;)
    {
        route.push_back(scenario.hosts[currentIndex]);
        const auto predecessorIndex = *predecessorByIndex[currentIndex];
        if (predecessorIndex == currentIndex)
        {
            break;
        }
        currentIndex = predecessorIndex;
    }
    std::ranges::reverse(route);
    return route;
}

[[nodiscard]] RecordingCandidateOption makeSyntheticCandidateOption(
    const RecordingSelectionDecision decision,
    const Host& host,
    const RecordingRepresentation representation,
    const double maintenanceCost,
    const size_t estimatedStorageBytes,
    const size_t incrementalStorageBytes,
    const uint64_t replayLatencyMs,
    const double replayTimeMultiplier,
    const size_t operatorCount,
    const uint64_t recordingNumber)
{
    return RecordingCandidateOption{
        .decision = decision,
        .selection = RecordingSelection{
            .recordingId = RecordingId("synthetic-recording-" + std::to_string(recordingNumber)),
            .node = host,
            .filePath = {},
            .structuralFingerprint = {},
            .representation = representation,
            .retentionWindowMs = std::nullopt,
            .beneficiaryQueries = {},
            .coversIncomingQuery = true},
        .cost = RecordingCostBreakdown{
            .decisionCost = maintenanceCost,
            .estimatedStorageBytes = estimatedStorageBytes,
            .incrementalStorageBytes = incrementalStorageBytes,
            .operatorCount = operatorCount,
            .estimatedReplayBandwidthBytesPerSecond = std::max<size_t>(4096, estimatedStorageBytes / 4),
            .estimatedReplayLatencyMs = replayLatencyMs,
            .maintenanceCost = maintenanceCost,
            .replayCost = maintenanceCost * 0.15,
            .recomputeCost = maintenanceCost * 0.10,
            .replayTimeMultiplier = replayTimeMultiplier,
            .boundaryCutCost = maintenanceCost,
            .replayRecomputeCost = maintenanceCost * 0.25,
            .fitsBudget = true,
            .satisfiesReplayLatency = true},
        .feasible = true,
        .infeasibilityReason = {}};
}

void applySelectedOptionMetrics(const ScenarioData& scenario, const RecordingCandidateOption& option, CutMetrics& metrics, const bool add)
{
    const auto direction = add ? 1.0 : -1.0;
    metrics.maintenanceCost += direction * option.cost.maintenanceCost;
    metrics.replayTimeMs += direction * weightedReplayScanTimeMs(option);
    if (option.decision != RecordingSelectionDecision::ReuseExistingRecording)
    {
        const auto hostIndex = findHostIndex(scenario.hosts, option.selection.node);
        if (add)
        {
            metrics.storageBytesByHost[hostIndex] += option.cost.incrementalStorageBytes;
        }
        else
        {
            metrics.storageBytesByHost[hostIndex] -= option.cost.incrementalStorageBytes;
        }
    }
    metrics.selectedEdgeCount = add ? metrics.selectedEdgeCount + 1 : metrics.selectedEdgeCount - 1;
}

[[nodiscard]] double sampleJitter(std::mt19937_64& randomEngine)
{
    return std::uniform_real_distribution<double>(0.90, 1.10)(randomEngine);
}

struct DagScenarioSpec
{
    DagScenarioKind kind;
    std::string_view optionName;
    std::string_view scenarioName;
};

[[nodiscard]] const std::vector<DagScenarioSpec>& dagScenarioRegistry()
{
    static const std::vector<DagScenarioSpec> registry = {
        DagScenarioSpec{
            .kind = DagScenarioKind::Diamond,
            .optionName = "diamond",
            .scenarioName = "dag-diamond-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::Join,
            .optionName = "join",
            .scenarioName = "dag-join-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::Pipeline,
            .optionName = "pipeline",
            .scenarioName = "dag-pipeline-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::Union,
            .optionName = "union",
            .scenarioName = "dag-union-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::MultiJoin,
            .optionName = "multi-join",
            .scenarioName = "dag-multi-join-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::JoinUnionHybrid,
            .optionName = "join-union-hybrid",
            .scenarioName = "dag-join-union-hybrid-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::SharedSubplan,
            .optionName = "shared-subplan",
            .scenarioName = "dag-shared-subplan-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::BranchingPipeline,
            .optionName = "branching-pipeline",
            .scenarioName = "dag-branching-pipeline-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::ReconvergingFanout,
            .optionName = "reconverging-fanout",
            .scenarioName = "dag-reconverging-fanout-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::MergedPipeline,
            .optionName = "merged-pipeline",
            .scenarioName = "dag-merged-pipeline-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::MergedUnion,
            .optionName = "merged-union",
            .scenarioName = "dag-merged-union-recording-selection"},
        DagScenarioSpec{
            .kind = DagScenarioKind::MergedJoin,
            .optionName = "merged-join",
            .scenarioName = "dag-merged-join-recording-selection"}};
    return registry;
}

[[nodiscard]] std::string_view dagScenarioName(const DagScenarioKind kind)
{
    for (const auto& spec : dagScenarioRegistry())
    {
        if (spec.kind == kind)
        {
            return spec.scenarioName;
        }
    }

    throw std::invalid_argument("unsupported DAG scenario kind");
}

[[nodiscard]] DagScenarioKind parseDagScenarioKind(const std::string_view input)
{
    for (const auto& spec : dagScenarioRegistry())
    {
        if (spec.optionName == input || spec.scenarioName == input)
        {
            return spec.kind;
        }
    }

    throw std::invalid_argument("unknown DAG family: " + std::string(input));
}

[[nodiscard]] std::vector<DagScenarioKind> parseDagScenarioKinds(const std::string_view input)
{
    const auto familyNames = parseStringList(input);
    std::vector<DagScenarioKind> families;
    families.reserve(familyNames.size());
    for (const auto& familyName : familyNames)
    {
        families.push_back(parseDagScenarioKind(familyName));
    }
    return families;
}

[[nodiscard]] std::vector<DagScenarioKind> configuredDagFamilies(const BenchmarkConfig& config)
{
    if (!config.dagFamilies.empty())
    {
        return config.dagFamilies;
    }

    std::vector<DagScenarioKind> families;
    families.reserve(dagScenarioRegistry().size());
    for (const auto& spec : dagScenarioRegistry())
    {
        families.push_back(spec.kind);
    }
    return families;
}

[[nodiscard]] std::vector<double> configuredDagFamilyWeights(const BenchmarkConfig& config, const std::vector<DagScenarioKind>& families)
{
    if (config.dagFamilyWeights.empty())
    {
        return std::vector<double>(families.size(), 1.0);
    }
    if (!config.dagFamilies.empty())
    {
        return config.dagFamilyWeights;
    }
    if (config.dagFamilyWeights.size() != dagScenarioRegistry().size())
    {
        throw std::invalid_argument("--dag-family-weights without --dag-families must cover all registered DAG families");
    }
    return config.dagFamilyWeights;
}

[[nodiscard]] DagScenarioKind chooseDagScenarioKind(const BenchmarkConfig& config, const size_t sizeParameter, const uint64_t seed)
{
    const auto families = configuredDagFamilies(config);
    const auto weights = configuredDagFamilyWeights(config, families);
    std::mt19937_64 randomEngine(seed ^ (static_cast<uint64_t>(sizeParameter) << 32U) ^ 0x51ed270b7c0deULL);
    std::discrete_distribution<size_t> familyDistribution(weights.begin(), weights.end());
    return families.at(familyDistribution(randomEngine));
}

[[nodiscard]] size_t effectiveScenarioHostCount(const BenchmarkConfig& config, const size_t sizeParameter, const uint64_t seed)
{
    if (config.scenarioFamily != ScenarioFamily::DagMix || config.hostCount <= 1)
    {
        return config.hostCount;
    }

    const auto availableExtraHosts = config.hostCount - 1;
    return 2 + ((sizeParameter + static_cast<size_t>(seed)) % availableExtraHosts);
}

[[nodiscard]] size_t performanceScenarioHostCount(const BenchmarkConfig& config, const size_t sizeParameter, const uint64_t seed)
{
    if (config.scenarioFamily != ScenarioFamily::DagMix)
    {
        return effectiveScenarioHostCount(config, sizeParameter, seed);
    }

    return std::max<size_t>(4, effectiveScenarioHostCount(config, sizeParameter, seed) + 1);
}

[[nodiscard]] std::vector<Host> makeBenchmarkHosts(const size_t hostCount)
{
    std::vector<Host> hosts;
    hosts.reserve(hostCount);
    for (size_t hostIndex = 0; hostIndex < hostCount; ++hostIndex)
    {
        hosts.emplace_back("benchmark-worker-" + std::to_string(hostIndex) + ":8080");
    }
    return hosts;
}

struct OperatorKindProfile
{
    double replayFactor = 1.0;
    double maintenanceFactor = 1.0;
    double storageFactor = 1.0;
    double bandwidthFactor = 1.0;
    double reusePenaltyFactor = 1.0;
    double createReplayBenefit = 1.0;
    double upgradeStorageFactor = 1.0;
    bool stateful = false;
    bool fanInHeavy = false;
};

[[nodiscard]] OperatorKindProfile operatorKindProfile(const std::string_view kind)
{
    if (kind == "source")
    {
        return OperatorKindProfile{
            .replayFactor = 0.85,
            .maintenanceFactor = 0.70,
            .storageFactor = 0.75,
            .bandwidthFactor = 0.85,
            .reusePenaltyFactor = 1.03,
            .createReplayBenefit = 0.96,
            .upgradeStorageFactor = 0.88,
            .stateful = false,
            .fanInHeavy = false};
    }
    if (kind == "filter" || kind == "project")
    {
        return OperatorKindProfile{
            .replayFactor = 0.92,
            .maintenanceFactor = 0.92,
            .storageFactor = 0.82,
            .bandwidthFactor = 0.88,
            .reusePenaltyFactor = 1.04,
            .createReplayBenefit = 0.95,
            .upgradeStorageFactor = 0.86,
            .stateful = false,
            .fanInHeavy = false};
    }
    if (kind == "map" || kind == "enrich")
    {
        return OperatorKindProfile{
            .replayFactor = kind == "enrich" ? 1.22 : 1.06,
            .maintenanceFactor = kind == "enrich" ? 1.18 : 1.04,
            .storageFactor = kind == "enrich" ? 1.08 : 0.96,
            .bandwidthFactor = kind == "enrich" ? 1.18 : 1.02,
            .reusePenaltyFactor = kind == "enrich" ? 1.12 : 1.05,
            .createReplayBenefit = kind == "enrich" ? 0.90 : 0.94,
            .upgradeStorageFactor = kind == "enrich" ? 0.82 : 0.84,
            .stateful = false,
            .fanInHeavy = false};
    }
    if (kind == "aggregate" || kind == "aggregate-keyed")
    {
        return OperatorKindProfile{
            .replayFactor = 1.58,
            .maintenanceFactor = 1.34,
            .storageFactor = 1.32,
            .bandwidthFactor = 1.10,
            .reusePenaltyFactor = 1.14,
            .createReplayBenefit = 0.86,
            .upgradeStorageFactor = 0.72,
            .stateful = true,
            .fanInHeavy = false};
    }
    if (kind == "aggregate-window")
    {
        return OperatorKindProfile{
            .replayFactor = 1.76,
            .maintenanceFactor = 1.46,
            .storageFactor = 1.48,
            .bandwidthFactor = 1.18,
            .reusePenaltyFactor = 1.18,
            .createReplayBenefit = 0.84,
            .upgradeStorageFactor = 0.68,
            .stateful = true,
            .fanInHeavy = false};
    }
    if (kind == "join" || kind == "join-hash")
    {
        return OperatorKindProfile{
            .replayFactor = 2.05,
            .maintenanceFactor = 1.68,
            .storageFactor = 1.62,
            .bandwidthFactor = 1.30,
            .reusePenaltyFactor = 1.22,
            .createReplayBenefit = 0.80,
            .upgradeStorageFactor = 0.66,
            .stateful = true,
            .fanInHeavy = true};
    }
    if (kind == "join-window")
    {
        return OperatorKindProfile{
            .replayFactor = 2.22,
            .maintenanceFactor = 1.82,
            .storageFactor = 1.84,
            .bandwidthFactor = 1.36,
            .reusePenaltyFactor = 1.26,
            .createReplayBenefit = 0.78,
            .upgradeStorageFactor = 0.62,
            .stateful = true,
            .fanInHeavy = true};
    }
    if (kind == "union")
    {
        return OperatorKindProfile{
            .replayFactor = 1.34,
            .maintenanceFactor = 1.12,
            .storageFactor = 0.90,
            .bandwidthFactor = 1.04,
            .reusePenaltyFactor = 1.08,
            .createReplayBenefit = 0.92,
            .upgradeStorageFactor = 0.82,
            .stateful = false,
            .fanInHeavy = true};
    }
    if (kind == "sink")
    {
        return OperatorKindProfile{
            .replayFactor = 0.64,
            .maintenanceFactor = 0.78,
            .storageFactor = 0.74,
            .bandwidthFactor = 0.70,
            .reusePenaltyFactor = 1.02,
            .createReplayBenefit = 0.98,
            .upgradeStorageFactor = 0.92,
            .stateful = false,
            .fanInHeavy = false};
    }

    throw std::invalid_argument("unknown operator kind: " + std::string(kind));
}

[[nodiscard]] double operatorReplayTimeForKind(
    const std::string_view kind, const size_t sizeParameter, const size_t depth, std::mt19937_64& randomEngine)
{
    const auto profile = operatorKindProfile(kind);
    auto base = (1.15 + (0.30 * static_cast<double>(sizeParameter)) + (0.42 * static_cast<double>(depth))) * profile.replayFactor;
    if (profile.stateful)
    {
        base *= 1.0 + (0.05 * static_cast<double>(std::max<size_t>(1, sizeParameter)));
    }
    if (profile.fanInHeavy)
    {
        base *= 1.10;
    }
    return base * sampleJitter(randomEngine);
}

size_t addScenarioNode(
    ScenarioData& scenario,
    uint64_t& nextOperatorId,
    const std::string_view kind,
    const double replayTimeMs,
    const bool isLeaf,
    const bool includeReplayTimeEntry = true)
{
    const auto nodeIndex = scenario.nodes.size();
    scenario.nodes.push_back(TreeNode{
        .id = OperatorId(nextOperatorId++),
        .kind = std::string(kind),
        .childEdgeIndices = {},
        .replayTimeMs = isLeaf ? 0.0 : replayTimeMs,
        .isLeaf = isLeaf});
    if (isLeaf)
    {
        scenario.candidateSet.leafOperatorIds.push_back(scenario.nodes.back().id);
    }
    else if (includeReplayTimeEntry)
    {
        scenario.candidateSet.operatorReplayTimes.push_back(
            RecordingCandidateSet::OperatorReplayTime{.operatorId = scenario.nodes.back().id, .replayTimeMs = replayTimeMs});
    }
    return nodeIndex;
}

void addScenarioPlanEdge(
    ScenarioData& scenario,
    const size_t parentNodeIndex,
    const size_t childNodeIndex,
    const size_t hostIndex,
    const std::vector<Host>& routeNodes,
    std::optional<std::vector<RecordingCandidateOption>> options = std::nullopt)
{
    const auto edge = RecordingPlanEdge{.parentId = scenario.nodes[parentNodeIndex].id, .childId = scenario.nodes[childNodeIndex].id};
    scenario.candidateSet.planEdges.push_back(edge);
    scenario.nodes[parentNodeIndex].childEdgeIndices.push_back(scenario.edges.size());
    scenario.edges.push_back(TreeEdge{
        .edge = edge,
        .parentNodeIndex = parentNodeIndex,
        .childNodeIndex = childNodeIndex,
        .hostIndex = hostIndex,
        .routeNodes = routeNodes,
        .candidate = std::nullopt});

    if (!options.has_value())
    {
        return;
    }

    auto candidate = RecordingBoundaryCandidate{
        .edge = edge,
        .upstreamNode = routeNodes.front(),
        .downstreamNode = routeNodes.back(),
        .routeNodes = routeNodes,
        .materializationEdges = {},
        .activeQueryMaterializationTargets = {},
        .beneficiaryQueries = {},
        .coversIncomingQuery = true,
        .options = std::move(*options)};
    scenario.edges.back().candidate = candidate;
    scenario.candidateSet.candidates.push_back(std::move(candidate));
}

void addScenarioEdge(
    ScenarioData& scenario,
    const size_t parentNodeIndex,
    const size_t childNodeIndex,
    const size_t hostIndex,
    const std::vector<Host>& routeNodes,
    std::vector<RecordingCandidateOption> options)
{
    addScenarioPlanEdge(scenario, parentNodeIndex, childNodeIndex, hostIndex, routeNodes, std::move(options));
}

[[nodiscard]] std::vector<RecordingCandidateOption> makeDagEdgeOptions(
    const ScenarioData& scenario,
    const std::string_view childKind,
    const std::string_view parentKind,
    const size_t upstreamHostIndex,
    const size_t downstreamHostIndex,
    const size_t structuralScale,
    const double maintenanceBias,
    std::mt19937_64& randomEngine,
    uint64_t& nextRecordingNumber)
{
    const auto childProfile = operatorKindProfile(childKind);
    const auto parentProfile = operatorKindProfile(parentKind);
    const auto routeNodes = routeBetweenHostIndices(scenario, upstreamHostIndex, downstreamHostIndex);
    const auto hopDistance = routeNodes.empty() ? 0U : static_cast<uint64_t>(routeNodes.size() - 1U);
    const auto structuralScaleDouble = static_cast<double>(std::max<size_t>(1, structuralScale));
    const auto statefulnessFactor = childProfile.stateful ? 1.24 : 1.0;
    const auto fanInFactor = parentProfile.fanInHeavy ? 1.16 : 1.0;
    const auto baseMaintenance
        = std::pow(structuralScaleDouble, 1.10) * maintenanceBias * childProfile.maintenanceFactor * fanInFactor
        * (1.0 + (0.08 * static_cast<double>(hopDistance))) * sampleJitter(randomEngine);
    const auto baseStorageBytes = static_cast<size_t>(std::ceil(
        (160.0 * 1024.0 * structuralScaleDouble * childProfile.storageFactor * statefulnessFactor)
        * (1.0 + (0.10 * static_cast<double>(hopDistance)) + (0.08 * (sampleJitter(randomEngine) - 1.0)))));
    const auto baseReplayLatencyMs = static_cast<uint64_t>(std::ceil(
        (4.2 + (2.0 * structuralScaleDouble * childProfile.replayFactor * fanInFactor))
        * (1.0 + (0.10 * static_cast<double>(hopDistance))) * sampleJitter(randomEngine)));

    const auto midpointIndex = routeNodes.empty() ? upstreamHostIndex : findHostIndex(scenario.hosts, routeNodes[routeNodes.size() / 2U]);
    std::vector<RecordingCandidateOption> options;
    options.reserve(4);
    options.push_back(makeSyntheticCandidateOption(
        RecordingSelectionDecision::ReuseExistingRecording,
        routeNodes.front(),
        RecordingRepresentation::BinaryStore,
        baseMaintenance * 0.22,
        static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * 0.70)),
        0,
        static_cast<uint64_t>(std::ceil(static_cast<double>(baseReplayLatencyMs) * childProfile.reusePenaltyFactor)),
        childProfile.reusePenaltyFactor + (0.03 * static_cast<double>(hopDistance)),
        structuralScale,
        nextRecordingNumber++));
    options.push_back(makeSyntheticCandidateOption(
        RecordingSelectionDecision::UpgradeExistingRecording,
        scenario.hosts[midpointIndex],
        RecordingRepresentation::BinaryStoreZstdFast1,
        baseMaintenance * 0.58,
        static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * 0.92)),
        static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * (1.0 - childProfile.upgradeStorageFactor))),
        static_cast<uint64_t>(std::ceil(static_cast<double>(baseReplayLatencyMs) * 0.92)),
        1.0,
        structuralScale,
        nextRecordingNumber++));
    options.push_back(makeSyntheticCandidateOption(
        RecordingSelectionDecision::CreateNewRecording,
        routeNodes.back(),
        RecordingRepresentation::BinaryStoreZstd,
        baseMaintenance * 0.90,
        static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * 0.72)),
        static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * 0.72)),
        static_cast<uint64_t>(std::ceil(static_cast<double>(baseReplayLatencyMs) * childProfile.createReplayBenefit)),
        childProfile.createReplayBenefit,
        structuralScale,
        nextRecordingNumber++));
    if (routeNodes.front() != routeNodes.back())
    {
        options.push_back(makeSyntheticCandidateOption(
            RecordingSelectionDecision::CreateNewRecording,
            routeNodes.front(),
            RecordingRepresentation::BinaryStore,
            baseMaintenance * (childProfile.stateful ? 0.82 : 0.76),
            static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * (1.05 + (0.10 * childProfile.bandwidthFactor)))),
            static_cast<size_t>(std::ceil(static_cast<double>(baseStorageBytes) * (1.05 + (0.10 * childProfile.bandwidthFactor)))),
            static_cast<uint64_t>(std::ceil(static_cast<double>(baseReplayLatencyMs) * (1.02 + (0.06 * parentProfile.bandwidthFactor)))),
            1.04 + (0.04 * parentProfile.bandwidthFactor),
            structuralScale,
            nextRecordingNumber++));
    }
    return options;
}

struct DagScenarioBuilder
{
    ScenarioData scenario;
    size_t sizeParameter = 0;
    size_t hostCount = 0;
    std::mt19937_64 randomEngine;
    uint64_t nextOperatorId = 1;
    uint64_t nextRecordingNumber = 0;

    DagScenarioBuilder(
        const DagScenarioKind kind,
        const size_t size,
        const size_t hosts,
        const uint64_t seed,
        const BaselineMode baselineMode,
        const NetworkTopology networkTopology)
        : sizeParameter(size), hostCount(hosts), randomEngine(seed)
    {
        scenario.scenarioName = std::string(dagScenarioName(kind));
        scenario.baselineMode = baselineMode;
        scenario.usesGenericExactEnumeration = true;
        initializeScenarioNetwork(scenario, hostCount, networkTopology, seed);
        scenario.storageBudgetsBytes.assign(hostCount, 0);
    }

    [[nodiscard]] bool exactMode() const { return scenario.baselineMode == BaselineMode::Exact; }

    [[nodiscard]] size_t hostModulo(const size_t rawIndex) const
    {
        return scenario.hosts.empty() ? 0U : std::min(rawIndex % scenario.hosts.size(), scenario.hosts.size() - 1U);
    }

    [[nodiscard]] size_t sinkHostIndex() const
    {
        return scenario.hosts.empty() ? 0U : scenario.hosts.size() - 1U;
    }

    [[nodiscard]] size_t midpointHostIndex() const
    {
        return scenario.hosts.empty() ? 0U : scenario.hosts.size() / 2U;
    }

    size_t addNode(const std::string_view nodeKind, const size_t depth, const bool isLeaf = false)
    {
        return addScenarioNode(
            scenario,
            nextOperatorId,
            nodeKind,
            operatorReplayTimeForKind(nodeKind, sizeParameter, depth, randomEngine),
            isLeaf);
    }

    size_t addStructuralNode(const std::string_view nodeKind)
    {
        return addScenarioNode(scenario, nextOperatorId, nodeKind, 0.0, false, false);
    }

    void addEdge(
        const size_t parentNodeIndex,
        const size_t childNodeIndex,
        const size_t childHostIndex,
        const size_t parentHostIndex,
        const size_t structuralScale,
        const double maintenanceBias)
    {
        const auto routeNodes = routeBetweenHostIndices(scenario, childHostIndex, parentHostIndex);
        addScenarioEdge(
            scenario,
            parentNodeIndex,
            childNodeIndex,
            childHostIndex,
            routeNodes,
            makeDagEdgeOptions(
                scenario,
                scenario.nodes[childNodeIndex].kind,
                scenario.nodes[parentNodeIndex].kind,
                childHostIndex,
                parentHostIndex,
                structuralScale,
                maintenanceBias,
                randomEngine,
                nextRecordingNumber));
    }

    void addNonCandidateEdge(
        const size_t parentNodeIndex, const size_t childNodeIndex, const size_t childHostIndex, const size_t parentHostIndex)
    {
        addScenarioPlanEdge(
            scenario,
            parentNodeIndex,
            childNodeIndex,
            childHostIndex,
            routeBetweenHostIndices(scenario, childHostIndex, parentHostIndex));
    }

    size_t addSyntheticRoot(const std::vector<size_t>& queryRootNodeIndices)
    {
        const auto syntheticRootIndex = addStructuralNode("synthetic-root");
        for (const auto queryRootNodeIndex : queryRootNodeIndices)
        {
            addNonCandidateEdge(syntheticRootIndex, queryRootNodeIndex, sinkHostIndex(), sinkHostIndex());
        }
        return syntheticRootIndex;
    }

    size_t addUnaryChain(
        const size_t startNodeIndex,
        const std::vector<std::string_view>& kinds,
        const size_t initialDepth,
        const size_t childHostSeed,
        const size_t parentHostSeed,
        const size_t structuralScaleBase,
        const double maintenanceBiasBase)
    {
        auto currentChild = startNodeIndex;
        for (size_t stage = 0; stage < kinds.size(); ++stage)
        {
            const auto nodeIndex = addNode(kinds[stage], std::max<size_t>(1, initialDepth - stage));
            addEdge(
                nodeIndex,
                currentChild,
                hostModulo(childHostSeed + stage),
                hostModulo(parentHostSeed + stage + 1),
                structuralScaleBase + stage,
                maintenanceBiasBase + (0.9 * static_cast<double>(stage)));
            currentChild = nodeIndex;
        }
        return currentChild;
    }
};

[[nodiscard]] size_t buildDiamondScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sharedLeaf = builder.addNode("source", 5, true);
        const auto leftFilter = builder.addNode("filter", 4);
        const auto rightProject = builder.addNode("project", 4);
        const auto unionNode = builder.addNode("union", 3);
        const auto aggregateNode = builder.addNode("aggregate-keyed", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(leftFilter, sharedLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 3), 7.0);
        builder.addEdge(rightProject, sharedLeaf, builder.hostModulo(0), builder.hostModulo(2), 2 + ((size + 1) % 3), 7.8);
        builder.addEdge(unionNode, leftFilter, builder.hostModulo(1), builder.hostModulo(2), 3 + (size % 2), 9.0);
        builder.addEdge(unionNode, rightProject, builder.hostModulo(2), builder.hostModulo(2), 3 + ((size + 1) % 2), 9.3);
        builder.addEdge(aggregateNode, unionNode, builder.hostModulo(2), sinkHost, 4 + (size % 2), 10.4);
        builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 5 + (size % 2), 11.2);
        return rootNode;
    }

    const auto sharedLeaf = builder.addNode("source", 8, true);
    const auto leftBranch = builder.addUnaryChain(sharedLeaf, {"filter", "project", "enrich"}, 7, 0, 1, 2 + (size % 3), 7.0);
    const auto rightBranch = builder.addUnaryChain(sharedLeaf, {"map", "aggregate-keyed"}, 7, 1, 2, 2 + ((size + 1) % 3), 8.0);
    const auto unionNode = builder.addNode("union", 4);
    builder.addEdge(unionNode, leftBranch, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 9.5);
    builder.addEdge(unionNode, rightBranch, builder.hostModulo(3), builder.hostModulo(3), 5 + ((size + 1) % 2), 9.8);
    const auto aggregateNode = builder.addNode("aggregate-window", 3);
    builder.addEdge(aggregateNode, unionNode, builder.hostModulo(3), sinkHost, 6 + (size % 3), 11.3);
    const auto projectNode = builder.addNode("project", 2);
    builder.addEdge(projectNode, aggregateNode, builder.hostModulo(4), sinkHost, 7 + (size % 2), 11.9);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, projectNode, sinkHost, sinkHost, 8 + (size % 2), 12.6);
    return rootNode;
}

[[nodiscard]] size_t buildJoinScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto leftLeaf = builder.addNode("source", 5, true);
        const auto leftFilter = builder.addNode("filter", 4);
        const auto leftAggregate = builder.addNode("aggregate-keyed", 3);
        const auto rightLeaf = builder.addNode("source", 5, true);
        const auto rightProject = builder.addNode("project", 4);
        const auto joinNode = builder.addNode("join-hash", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(leftFilter, leftLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 3), 7.2);
        builder.addEdge(leftAggregate, leftFilter, builder.hostModulo(1), builder.hostModulo(1), 3 + (size % 2), 8.7);
        builder.addEdge(rightProject, rightLeaf, builder.hostModulo(2), builder.hostModulo(2), 2 + ((size + 1) % 3), 7.4);
        builder.addEdge(joinNode, leftAggregate, builder.hostModulo(1), builder.hostModulo(2), 4 + (size % 2), 12.2);
        builder.addEdge(joinNode, rightProject, builder.hostModulo(2), builder.hostModulo(2), 3 + ((size + 1) % 2), 11.3);
        builder.addEdge(rootNode, joinNode, builder.hostModulo(2), sinkHost, 5 + (size % 2), 12.0);
        return rootNode;
    }

    const auto leftLeaf = builder.addNode("source", 8, true);
    const auto leftBranch =
        builder.addUnaryChain(leftLeaf, {"filter", "project", "aggregate-keyed"}, 7, 0, 1, 2 + (size % 3), 7.0);
    const auto rightLeaf = builder.addNode("source", 8, true);
    const auto rightBranch =
        builder.addUnaryChain(rightLeaf, {"map", "enrich", "aggregate-window"}, 7, 2, 3, 2 + ((size + 1) % 3), 7.8);
    const auto joinNode = builder.addNode("join-window", 4);
    builder.addEdge(joinNode, leftBranch, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 12.6);
    builder.addEdge(joinNode, rightBranch, builder.hostModulo(3), builder.hostModulo(3), 5 + ((size + 1) % 2), 12.1);
    const auto postProject = builder.addNode("project", 3);
    builder.addEdge(postProject, joinNode, builder.hostModulo(4), builder.hostModulo(1), 6 + (size % 2), 10.9);
    const auto postAggregate = builder.addNode("aggregate-window", 2);
    builder.addEdge(postAggregate, postProject, builder.hostModulo(1), sinkHost, 7 + (size % 2), 11.8);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, postAggregate, sinkHost, sinkHost, 8 + (size % 2), 12.4);
    return rootNode;
}

[[nodiscard]] size_t buildPipelineScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sourceLeaf = builder.addNode("source", 5, true);
        const auto filterNode = builder.addNode("filter", 4);
        const auto mapNode = builder.addNode("map", 3);
        const auto aggregateNode = builder.addNode("aggregate-window", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(filterNode, sourceLeaf, builder.hostModulo(0), builder.hostModulo(0), 2 + (size % 2), 6.5);
        builder.addEdge(mapNode, filterNode, builder.hostModulo(0), builder.hostModulo(1), 3 + (size % 2), 7.8);
        builder.addEdge(aggregateNode, mapNode, builder.hostModulo(1), builder.hostModulo(2), 4 + (size % 2), 9.3);
        builder.addEdge(rootNode, aggregateNode, builder.hostModulo(2), sinkHost, 5 + (size % 2), 10.4);
        return rootNode;
    }

    const auto sourceLeaf = builder.addNode("source", size + builder.hostCount + 4, true);
    auto currentNode = sourceLeaf;
    const std::vector<std::string_view> stageKinds = {
        "filter", "project", "map", "enrich", "aggregate-keyed", "map", "aggregate-window"};
    const auto stageCount = std::max<size_t>(6, size + builder.hostCount);
    for (size_t stage = 0; stage < stageCount; ++stage)
    {
        const auto stageNode = builder.addNode(stageKinds[stage % stageKinds.size()], std::max<size_t>(2, stageCount - stage + 2));
        builder.addEdge(
            stageNode,
            currentNode,
            builder.hostModulo(stage),
            builder.hostModulo(stage + 1),
            2 + (stage % 4) + (size % 3),
            6.8 + static_cast<double>(stage));
        currentNode = stageNode;
    }
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, currentNode, builder.hostModulo(stageCount + 1), sinkHost, 7 + (size % 2), 11.8);
    return rootNode;
}

[[nodiscard]] size_t buildUnionScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto leftLeaf = builder.addNode("source", 5, true);
        const auto leftProject = builder.addNode("project", 4);
        const auto rightLeaf = builder.addNode("source", 5, true);
        const auto rightFilter = builder.addNode("filter", 4);
        const auto unionNode = builder.addNode("union", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(leftProject, leftLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 3), 7.2);
        builder.addEdge(rightFilter, rightLeaf, builder.hostModulo(2), builder.hostModulo(1), 2 + ((size + 1) % 3), 6.9);
        builder.addEdge(unionNode, leftProject, builder.hostModulo(1), builder.hostModulo(2), 4 + (size % 2), 9.4);
        builder.addEdge(unionNode, rightFilter, builder.hostModulo(1), builder.hostModulo(2), 3 + ((size + 1) % 2), 9.0);
        builder.addEdge(rootNode, unionNode, builder.hostModulo(2), sinkHost, 5 + (size % 2), 10.2);
        return rootNode;
    }

    const auto branchCount = 3 + (size % 3) + std::min<size_t>(2, builder.hostCount / 3);
    std::vector<size_t> branchHeads;
    branchHeads.reserve(branchCount);
    const std::vector<std::string_view> branchKinds = {"map", "filter", "project", "aggregate-keyed"};
    for (size_t branchIndex = 0; branchIndex < branchCount; ++branchIndex)
    {
        const auto leafNode = builder.addNode("source", 7, true);
        auto currentBranchNode = leafNode;
        const auto branchStages = 2 + ((branchIndex + size) % 2);
        for (size_t stage = 0; stage < branchStages; ++stage)
        {
            const auto stageNode = builder.addNode(branchKinds[(branchIndex + stage) % branchKinds.size()], 6 - stage);
            builder.addEdge(
                stageNode,
                currentBranchNode,
                builder.hostModulo(branchIndex + stage),
                builder.hostModulo(branchIndex + stage + 1),
                2 + stage + (branchIndex % 2),
                7.4 + static_cast<double>(branchIndex) + (0.7 * static_cast<double>(stage)));
            currentBranchNode = stageNode;
        }
        branchHeads.push_back(currentBranchNode);
    }

    const auto unionNode = builder.addNode("union", 3);
    for (size_t branchIndex = 0; branchIndex < branchHeads.size(); ++branchIndex)
    {
        builder.addEdge(
            unionNode,
            branchHeads[branchIndex],
            builder.hostModulo(branchIndex + 1),
            builder.hostModulo(builder.midpointHostIndex()),
            4 + (branchIndex % 3),
            9.0 + static_cast<double>(branchIndex));
    }
    const auto aggregateNode = builder.addNode("aggregate-window", 2);
    builder.addEdge(aggregateNode, unionNode, builder.hostModulo(branchCount + 1), sinkHost, 6 + (size % 2), 10.9);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, aggregateNode, builder.hostModulo(branchCount + 2), sinkHost, 7 + (size % 2), 11.6);
    return rootNode;
}

[[nodiscard]] size_t buildMultiJoinScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto leftLeaf = builder.addNode("source", 6, true);
        const auto leftFilter = builder.addNode("filter", 5);
        const auto middleLeaf = builder.addNode("source", 6, true);
        const auto middleProject = builder.addNode("project", 5);
        const auto firstJoin = builder.addNode("join-hash", 3);
        const auto rightLeaf = builder.addNode("source", 6, true);
        const auto rightMap = builder.addNode("map", 5);
        const auto secondJoin = builder.addNode("join-window", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(leftFilter, leftLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 7.1);
        builder.addEdge(middleProject, middleLeaf, builder.hostModulo(1), builder.hostModulo(2), 2 + ((size + 1) % 2), 7.4);
        builder.addEdge(firstJoin, leftFilter, builder.hostModulo(1), builder.hostModulo(2), 4 + (size % 2), 11.8);
        builder.addEdge(firstJoin, middleProject, builder.hostModulo(2), builder.hostModulo(2), 4 + ((size + 1) % 2), 11.6);
        builder.addEdge(rightMap, rightLeaf, builder.hostModulo(2), builder.hostModulo(3), 2 + ((size + 2) % 2), 7.6);
        builder.addEdge(secondJoin, firstJoin, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 12.4);
        builder.addEdge(secondJoin, rightMap, builder.hostModulo(3), builder.hostModulo(3), 4 + ((size + 1) % 2), 11.9);
        builder.addEdge(rootNode, secondJoin, builder.hostModulo(3), sinkHost, 6 + (size % 2), 12.7);
        return rootNode;
    }

    const auto leftLeaf = builder.addNode("source", 8, true);
    const auto leftBranch = builder.addUnaryChain(leftLeaf, {"filter", "aggregate-keyed"}, 7, 0, 1, 2 + (size % 3), 7.0);
    const auto middleLeaf = builder.addNode("source", 8, true);
    const auto middleBranch = builder.addUnaryChain(middleLeaf, {"project", "enrich"}, 7, 1, 2, 2 + ((size + 1) % 3), 7.5);
    const auto firstJoin = builder.addNode("join-hash", 5);
    builder.addEdge(firstJoin, leftBranch, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 12.4);
    builder.addEdge(firstJoin, middleBranch, builder.hostModulo(3), builder.hostModulo(3), 5 + ((size + 1) % 2), 12.1);
    const auto rightLeaf = builder.addNode("source", 8, true);
    const auto rightBranch = builder.addUnaryChain(rightLeaf, {"map", "aggregate-window"}, 7, 3, 4, 3 + ((size + 2) % 3), 7.9);
    const auto finalJoin = builder.addNode("join-window", 4);
    builder.addEdge(finalJoin, firstJoin, builder.hostModulo(3), builder.hostModulo(4), 6 + (size % 2), 13.2);
    builder.addEdge(finalJoin, rightBranch, builder.hostModulo(4), builder.hostModulo(4), 6 + ((size + 1) % 2), 12.6);
    const auto aggregateNode = builder.addNode("aggregate-window", 2);
    builder.addEdge(aggregateNode, finalJoin, builder.hostModulo(4), sinkHost, 7 + (size % 2), 11.8);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 8 + (size % 2), 12.5);
    return rootNode;
}

[[nodiscard]] size_t buildJoinUnionHybridScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto leftLeaf = builder.addNode("source", 6, true);
        const auto leftProject = builder.addNode("project", 5);
        const auto middleLeaf = builder.addNode("source", 6, true);
        const auto middleFilter = builder.addNode("filter", 5);
        const auto unionNode = builder.addNode("union", 4);
        const auto rightLeaf = builder.addNode("source", 6, true);
        const auto rightMap = builder.addNode("map", 5);
        const auto joinNode = builder.addNode("join-hash", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(leftProject, leftLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 7.0);
        builder.addEdge(middleFilter, middleLeaf, builder.hostModulo(1), builder.hostModulo(1), 2 + ((size + 1) % 2), 7.2);
        builder.addEdge(unionNode, leftProject, builder.hostModulo(1), builder.hostModulo(2), 3 + (size % 2), 9.1);
        builder.addEdge(unionNode, middleFilter, builder.hostModulo(1), builder.hostModulo(2), 3 + ((size + 1) % 2), 9.4);
        builder.addEdge(rightMap, rightLeaf, builder.hostModulo(2), builder.hostModulo(3), 2 + ((size + 2) % 2), 7.6);
        builder.addEdge(joinNode, unionNode, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 12.0);
        builder.addEdge(joinNode, rightMap, builder.hostModulo(3), builder.hostModulo(3), 4 + ((size + 1) % 2), 11.5);
        builder.addEdge(rootNode, joinNode, builder.hostModulo(3), sinkHost, 6 + (size % 2), 12.3);
        return rootNode;
    }

    const auto leftLeaf = builder.addNode("source", 8, true);
    const auto leftBranch = builder.addUnaryChain(leftLeaf, {"project", "aggregate-keyed"}, 7, 0, 1, 2 + (size % 3), 7.1);
    const auto middleLeaf = builder.addNode("source", 8, true);
    const auto middleBranch = builder.addUnaryChain(middleLeaf, {"filter", "enrich"}, 7, 1, 2, 2 + ((size + 1) % 3), 7.4);
    const auto unionNode = builder.addNode("union", 5);
    builder.addEdge(unionNode, leftBranch, builder.hostModulo(2), builder.hostModulo(3), 4 + (size % 2), 9.8);
    builder.addEdge(unionNode, middleBranch, builder.hostModulo(3), builder.hostModulo(3), 4 + ((size + 1) % 2), 9.6);
    const auto rightLeaf = builder.addNode("source", 8, true);
    const auto rightBranch = builder.addUnaryChain(rightLeaf, {"map", "aggregate-window"}, 7, 3, 4, 3 + ((size + 2) % 3), 7.9);
    const auto joinNode = builder.addNode("join-window", 3);
    builder.addEdge(joinNode, unionNode, builder.hostModulo(3), builder.hostModulo(4), 6 + (size % 2), 12.8);
    builder.addEdge(joinNode, rightBranch, builder.hostModulo(4), builder.hostModulo(4), 6 + ((size + 1) % 2), 12.2);
    const auto aggregateNode = builder.addNode("aggregate-window", 2);
    builder.addEdge(aggregateNode, joinNode, builder.hostModulo(4), sinkHost, 7 + (size % 2), 11.7);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 8 + (size % 2), 12.5);
    return rootNode;
}

[[nodiscard]] size_t buildSharedSubplanScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sourceLeaf = builder.addNode("source", 6, true);
        const auto sharedFilter = builder.addNode("filter", 5);
        const auto sharedProject = builder.addNode("project", 4);
        const auto aggregateConsumer = builder.addNode("aggregate-keyed", 3);
        const auto enrichConsumer = builder.addNode("enrich", 3);
        const auto unionNode = builder.addNode("union", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(sharedFilter, sourceLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 6.8);
        builder.addEdge(sharedProject, sharedFilter, builder.hostModulo(1), builder.hostModulo(2), 3 + (size % 2), 7.6);
        builder.addEdge(aggregateConsumer, sharedProject, builder.hostModulo(2), builder.hostModulo(2), 4 + (size % 2), 9.8);
        builder.addEdge(enrichConsumer, sharedProject, builder.hostModulo(2), builder.hostModulo(3), 4 + ((size + 1) % 2), 9.4);
        builder.addEdge(unionNode, aggregateConsumer, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 10.5);
        builder.addEdge(unionNode, enrichConsumer, builder.hostModulo(3), builder.hostModulo(3), 5 + ((size + 1) % 2), 10.1);
        builder.addEdge(rootNode, unionNode, builder.hostModulo(3), sinkHost, 6 + (size % 2), 11.4);
        return rootNode;
    }

    const auto sourceLeaf = builder.addNode("source", 8, true);
    const auto sharedHead = builder.addUnaryChain(sourceLeaf, {"filter", "project", "enrich"}, 7, 0, 1, 2 + (size % 3), 6.8);
    const auto aggregateConsumer = builder.addNode("aggregate-window", 4);
    builder.addEdge(aggregateConsumer, sharedHead, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 10.8);
    const auto sideLeaf = builder.addNode("source", 7, true);
    const auto sideBranch = builder.addUnaryChain(sideLeaf, {"map"}, 6, 3, 4, 3 + ((size + 1) % 3), 7.5);
    const auto joinConsumer = builder.addNode("join-hash", 3);
    builder.addEdge(joinConsumer, sharedHead, builder.hostModulo(2), builder.hostModulo(4), 6 + (size % 2), 12.4);
    builder.addEdge(joinConsumer, sideBranch, builder.hostModulo(4), builder.hostModulo(4), 5 + ((size + 1) % 2), 11.7);
    const auto unionNode = builder.addNode("union", 2);
    builder.addEdge(unionNode, aggregateConsumer, builder.hostModulo(3), sinkHost, 7 + (size % 2), 11.2);
    builder.addEdge(unionNode, joinConsumer, builder.hostModulo(4), sinkHost, 7 + ((size + 1) % 2), 11.8);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, unionNode, sinkHost, sinkHost, 8 + (size % 2), 12.4);
    return rootNode;
}

[[nodiscard]] size_t buildBranchingPipelineScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sourceLeaf = builder.addNode("source", 6, true);
        const auto filterNode = builder.addNode("filter", 5);
        const auto leftProject = builder.addNode("project", 4);
        const auto rightMap = builder.addNode("map", 4);
        const auto unionNode = builder.addNode("union", 3);
        const auto aggregateNode = builder.addNode("aggregate-window", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(filterNode, sourceLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 6.9);
        builder.addEdge(leftProject, filterNode, builder.hostModulo(1), builder.hostModulo(2), 3 + (size % 2), 7.8);
        builder.addEdge(rightMap, filterNode, builder.hostModulo(1), builder.hostModulo(3), 3 + ((size + 1) % 2), 8.1);
        builder.addEdge(unionNode, leftProject, builder.hostModulo(2), builder.hostModulo(3), 4 + (size % 2), 9.6);
        builder.addEdge(unionNode, rightMap, builder.hostModulo(3), builder.hostModulo(3), 4 + ((size + 1) % 2), 9.4);
        builder.addEdge(aggregateNode, unionNode, builder.hostModulo(3), sinkHost, 5 + (size % 2), 10.8);
        builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 6 + (size % 2), 11.5);
        return rootNode;
    }

    const auto sourceLeaf = builder.addNode("source", 8, true);
    const auto sharedHead = builder.addUnaryChain(sourceLeaf, {"filter", "project"}, 7, 0, 1, 2 + (size % 3), 6.9);
    const auto leftBranch = builder.addUnaryChain(sharedHead, {"map", "enrich"}, 5, 2, 3, 4 + (size % 2), 8.2);
    const auto rightBranch = builder.addUnaryChain(sharedHead, {"aggregate-keyed"}, 5, 2, 4, 4 + ((size + 1) % 2), 8.6);
    const auto unionNode = builder.addNode("union", 3);
    builder.addEdge(unionNode, leftBranch, builder.hostModulo(3), builder.hostModulo(4), 6 + (size % 2), 10.2);
    builder.addEdge(unionNode, rightBranch, builder.hostModulo(4), builder.hostModulo(4), 6 + ((size + 1) % 2), 10.6);
    const auto aggregateNode = builder.addNode("aggregate-window", 2);
    builder.addEdge(aggregateNode, unionNode, builder.hostModulo(4), sinkHost, 7 + (size % 2), 11.2);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 8 + (size % 2), 12.0);
    return rootNode;
}

[[nodiscard]] size_t buildReconvergingFanoutScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sourceLeaf = builder.addNode("source", 6, true);
        const auto branchA = builder.addNode("map", 5);
        const auto branchB = builder.addNode("filter", 5);
        const auto branchC = builder.addNode("project", 5);
        const auto firstUnion = builder.addNode("union", 3);
        const auto secondUnion = builder.addNode("union", 2);
        const auto rootNode = builder.addNode("sink", 1);

        builder.addEdge(branchA, sourceLeaf, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 7.0);
        builder.addEdge(branchB, sourceLeaf, builder.hostModulo(0), builder.hostModulo(2), 2 + ((size + 1) % 2), 7.1);
        builder.addEdge(branchC, sourceLeaf, builder.hostModulo(0), builder.hostModulo(3), 2 + ((size + 2) % 2), 7.3);
        builder.addEdge(firstUnion, branchA, builder.hostModulo(1), builder.hostModulo(3), 4 + (size % 2), 9.5);
        builder.addEdge(firstUnion, branchB, builder.hostModulo(2), builder.hostModulo(3), 4 + ((size + 1) % 2), 9.2);
        builder.addEdge(secondUnion, firstUnion, builder.hostModulo(3), builder.hostModulo(4), 5 + (size % 2), 10.4);
        builder.addEdge(secondUnion, branchC, builder.hostModulo(3), builder.hostModulo(4), 5 + ((size + 1) % 2), 10.0);
        builder.addEdge(rootNode, secondUnion, builder.hostModulo(4), sinkHost, 6 + (size % 2), 11.4);
        return rootNode;
    }

    const auto sourceLeaf = builder.addNode("source", 8, true);
    const auto branchA = builder.addUnaryChain(sourceLeaf, {"map", "aggregate-keyed"}, 7, 0, 1, 2 + (size % 3), 7.1);
    const auto branchB = builder.addUnaryChain(sourceLeaf, {"filter", "project"}, 7, 0, 2, 2 + ((size + 1) % 3), 7.2);
    const auto branchC = builder.addUnaryChain(sourceLeaf, {"enrich"}, 7, 0, 3, 2 + ((size + 2) % 3), 7.8);
    const auto firstUnion = builder.addNode("union", 4);
    builder.addEdge(firstUnion, branchA, builder.hostModulo(2), builder.hostModulo(4), 5 + (size % 2), 10.1);
    builder.addEdge(firstUnion, branchB, builder.hostModulo(3), builder.hostModulo(4), 5 + ((size + 1) % 2), 9.8);
    const auto secondUnion = builder.addNode("union", 3);
    builder.addEdge(secondUnion, firstUnion, builder.hostModulo(4), builder.hostModulo(5), 6 + (size % 2), 10.9);
    builder.addEdge(secondUnion, branchC, builder.hostModulo(3), builder.hostModulo(5), 6 + ((size + 1) % 2), 10.6);
    const auto aggregateNode = builder.addNode("aggregate-window", 2);
    builder.addEdge(aggregateNode, secondUnion, builder.hostModulo(5), sinkHost, 7 + (size % 2), 11.7);
    const auto rootNode = builder.addNode("sink", 1);
    builder.addEdge(rootNode, aggregateNode, sinkHost, sinkHost, 8 + (size % 2), 12.4);
    return rootNode;
}

[[nodiscard]] size_t buildMergedPipelineScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sharedSource = builder.addNode("source", 6, true);
        const auto sharedHead = builder.addUnaryChain(sharedSource, {"filter", "project"}, 5, 0, 1, 2 + (size % 2), 6.8);

        const auto aggregateBranch = builder.addNode("aggregate-window", 3);
        builder.addEdge(aggregateBranch, sharedHead, builder.hostModulo(2), builder.hostModulo(3), 4 + (size % 2), 10.6);
        const auto sinkOne = builder.addNode("sink", 2);
        builder.addEdge(sinkOne, aggregateBranch, builder.hostModulo(3), sinkHost, 5 + (size % 2), 11.5);

        const auto mapBranch = builder.addNode("map", 3);
        builder.addEdge(mapBranch, sharedHead, builder.hostModulo(2), builder.hostModulo(4), 4 + ((size + 1) % 2), 9.3);
        const auto keyedAggregate = builder.addNode("aggregate-keyed", 2);
        builder.addEdge(keyedAggregate, mapBranch, builder.hostModulo(4), sinkHost, 5 + ((size + 1) % 2), 10.8);
        const auto sinkTwo = builder.addNode("sink", 1);
        builder.addEdge(sinkTwo, keyedAggregate, sinkHost, sinkHost, 6 + (size % 2), 11.7);

        return builder.addSyntheticRoot({sinkOne, sinkTwo});
    }

    const auto sharedSource = builder.addNode("source", size + builder.hostCount + 6, true);
    auto sharedTail = sharedSource;
    const std::vector<std::string_view> stageKinds = {
        "filter", "project", "map", "enrich", "aggregate-keyed", "project", "map"};
    const auto stageCount = std::max<size_t>(8, size + (builder.hostCount / 2));
    for (size_t stage = 0; stage < stageCount; ++stage)
    {
        const auto stageNode = builder.addNode(stageKinds[stage % stageKinds.size()], std::max<size_t>(3, stageCount - stage + 3));
        builder.addEdge(
            stageNode,
            sharedTail,
            builder.hostModulo(stage),
            builder.hostModulo(stage + 1),
            2 + (stage % 4) + (size % 3),
            6.8 + static_cast<double>(stage));
        sharedTail = stageNode;
    }

    const auto aggregateBranch = builder.addNode("aggregate-window", 4);
    builder.addEdge(aggregateBranch, sharedTail, builder.hostModulo(stageCount + 1), builder.hostModulo(stageCount + 2), 5 + (size % 2), 11.1);
    const auto sinkOne = builder.addNode("sink", 2);
    builder.addEdge(sinkOne, aggregateBranch, builder.hostModulo(stageCount + 2), sinkHost, 6 + (size % 2), 11.9);

    const auto branchTwoHead = builder.addUnaryChain(sharedTail, {"enrich", "aggregate-keyed"}, 4, stageCount + 1, stageCount + 3, 4 + (size % 2), 8.8);
    const auto sinkTwo = builder.addNode("sink", 2);
    builder.addEdge(sinkTwo, branchTwoHead, builder.hostModulo(stageCount + 3), sinkHost, 6 + ((size + 1) % 2), 11.8);

    const auto sideSource = builder.addNode("source", 9, true);
    const auto sideBranch = builder.addUnaryChain(sideSource, {"filter", "project"}, 7, stageCount + 2, stageCount + 4, 3 + ((size + 1) % 3), 7.4);
    const auto joinBranch = builder.addNode("join-window", 3);
    builder.addEdge(joinBranch, sharedTail, builder.hostModulo(stageCount + 2), builder.hostModulo(stageCount + 5), 6 + (size % 2), 12.7);
    builder.addEdge(joinBranch, sideBranch, builder.hostModulo(stageCount + 4), builder.hostModulo(stageCount + 5), 5 + ((size + 1) % 2), 12.3);
    const auto sinkThree = builder.addNode("sink", 1);
    builder.addEdge(sinkThree, joinBranch, builder.hostModulo(stageCount + 5), sinkHost, 7 + (size % 2), 12.4);

    return builder.addSyntheticRoot({sinkOne, sinkTwo, sinkThree});
}

[[nodiscard]] size_t buildMergedUnionScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto leftSource = builder.addNode("source", 6, true);
        const auto leftBranch = builder.addNode("map", 5);
        builder.addEdge(leftBranch, leftSource, builder.hostModulo(0), builder.hostModulo(1), 2 + (size % 2), 7.2);

        const auto rightSource = builder.addNode("source", 6, true);
        const auto rightBranch = builder.addNode("filter", 5);
        builder.addEdge(rightBranch, rightSource, builder.hostModulo(2), builder.hostModulo(1), 2 + ((size + 1) % 2), 7.0);

        const auto unionNode = builder.addNode("union", 4);
        builder.addEdge(unionNode, leftBranch, builder.hostModulo(1), builder.hostModulo(2), 4 + (size % 2), 9.4);
        builder.addEdge(unionNode, rightBranch, builder.hostModulo(1), builder.hostModulo(2), 4 + ((size + 1) % 2), 9.1);

        const auto aggregateBranch = builder.addNode("aggregate-window", 3);
        builder.addEdge(aggregateBranch, unionNode, builder.hostModulo(2), builder.hostModulo(3), 5 + (size % 2), 10.7);
        const auto sinkOne = builder.addNode("sink", 2);
        builder.addEdge(sinkOne, aggregateBranch, builder.hostModulo(3), sinkHost, 6 + (size % 2), 11.5);

        const auto projectBranch = builder.addNode("project", 3);
        builder.addEdge(projectBranch, unionNode, builder.hostModulo(2), builder.hostModulo(4), 5 + ((size + 1) % 2), 9.8);
        const auto sinkTwo = builder.addNode("sink", 1);
        builder.addEdge(sinkTwo, projectBranch, builder.hostModulo(4), sinkHost, 6 + ((size + 1) % 2), 11.1);

        return builder.addSyntheticRoot({sinkOne, sinkTwo});
    }

    const auto branchCount = 4 + (size % 3) + std::min<size_t>(2, builder.hostCount / 4);
    std::vector<size_t> branchHeads;
    branchHeads.reserve(branchCount);
    const std::vector<std::string_view> branchKinds = {"map", "filter", "project", "aggregate-keyed"};
    for (size_t branchIndex = 0; branchIndex < branchCount; ++branchIndex)
    {
        const auto sourceNode = builder.addNode("source", 8, true);
        auto branchTail = sourceNode;
        const auto branchStages = 2 + ((branchIndex + size) % 3);
        for (size_t stage = 0; stage < branchStages; ++stage)
        {
            const auto stageNode = builder.addNode(branchKinds[(branchIndex + stage) % branchKinds.size()], 7 - std::min<size_t>(stage, 4));
            builder.addEdge(
                stageNode,
                branchTail,
                builder.hostModulo(branchIndex + stage),
                builder.hostModulo(branchIndex + stage + 1),
                2 + stage + (branchIndex % 2),
                7.2 + static_cast<double>(branchIndex) + (0.8 * static_cast<double>(stage)));
            branchTail = stageNode;
        }
        branchHeads.push_back(branchTail);
    }

    const auto unionNode = builder.addNode("union", 5);
    for (size_t branchIndex = 0; branchIndex < branchHeads.size(); ++branchIndex)
    {
        builder.addEdge(
            unionNode,
            branchHeads[branchIndex],
            builder.hostModulo(branchIndex + 1),
            builder.hostModulo(builder.midpointHostIndex()),
            4 + (branchIndex % 3),
            9.2 + static_cast<double>(branchIndex));
    }

    const auto sharedAggregate = builder.addNode("aggregate-window", 4);
    builder.addEdge(sharedAggregate, unionNode, builder.hostModulo(branchCount + 1), builder.hostModulo(branchCount + 2), 6 + (size % 2), 10.8);

    const auto sinkBranchOne = builder.addNode("project", 3);
    builder.addEdge(sinkBranchOne, sharedAggregate, builder.hostModulo(branchCount + 2), sinkHost, 7 + (size % 2), 10.9);
    const auto sinkOne = builder.addNode("sink", 2);
    builder.addEdge(sinkOne, sinkBranchOne, sinkHost, sinkHost, 8 + (size % 2), 11.6);

    const auto sinkBranchTwo = builder.addUnaryChain(sharedAggregate, {"enrich", "aggregate-keyed"}, 4, branchCount + 1, branchCount + 3, 5 + (size % 2), 8.7);
    const auto sinkTwo = builder.addNode("sink", 2);
    builder.addEdge(sinkTwo, sinkBranchTwo, builder.hostModulo(branchCount + 3), sinkHost, 8 + ((size + 1) % 2), 11.7);

    const auto sinkBranchThree = builder.addNode("map", 3);
    builder.addEdge(sinkBranchThree, unionNode, builder.hostModulo(branchCount + 1), sinkHost, 6 + ((size + 1) % 2), 9.9);
    const auto sinkThree = builder.addNode("sink", 1);
    builder.addEdge(sinkThree, sinkBranchThree, sinkHost, sinkHost, 7 + ((size + 1) % 2), 11.3);

    return builder.addSyntheticRoot({sinkOne, sinkTwo, sinkThree});
}

[[nodiscard]] size_t buildMergedJoinScenario(DagScenarioBuilder& builder)
{
    const auto size = builder.sizeParameter;
    const auto sinkHost = builder.sinkHostIndex();
    if (builder.exactMode())
    {
        const auto sharedSource = builder.addNode("source", 6, true);
        const auto sharedHead = builder.addUnaryChain(sharedSource, {"filter"}, 5, 0, 1, 2 + (size % 2), 7.0);

        const auto firstSideSource = builder.addNode("source", 6, true);
        const auto firstSideBranch = builder.addNode("project", 5);
        builder.addEdge(firstSideBranch, firstSideSource, builder.hostModulo(2), builder.hostModulo(2), 2 + ((size + 1) % 2), 7.4);
        const auto firstJoin = builder.addNode("join-hash", 3);
        builder.addEdge(firstJoin, sharedHead, builder.hostModulo(1), builder.hostModulo(3), 4 + (size % 2), 12.1);
        builder.addEdge(firstJoin, firstSideBranch, builder.hostModulo(2), builder.hostModulo(3), 4 + ((size + 1) % 2), 11.8);
        const auto sinkOne = builder.addNode("sink", 2);
        builder.addEdge(sinkOne, firstJoin, builder.hostModulo(3), sinkHost, 5 + (size % 2), 12.0);

        const auto secondSideSource = builder.addNode("source", 6, true);
        const auto secondSideBranch = builder.addNode("map", 5);
        builder.addEdge(secondSideBranch, secondSideSource, builder.hostModulo(3), builder.hostModulo(4), 2 + ((size + 2) % 2), 7.6);
        const auto secondJoin = builder.addNode("join-window", 3);
        builder.addEdge(secondJoin, sharedHead, builder.hostModulo(1), builder.hostModulo(4), 4 + (size % 2), 12.4);
        builder.addEdge(secondJoin, secondSideBranch, builder.hostModulo(3), builder.hostModulo(4), 4 + ((size + 1) % 2), 12.0);
        const auto sinkTwo = builder.addNode("sink", 1);
        builder.addEdge(sinkTwo, secondJoin, builder.hostModulo(4), sinkHost, 5 + ((size + 1) % 2), 12.2);

        return builder.addSyntheticRoot({sinkOne, sinkTwo});
    }

    const auto sharedSource = builder.addNode("source", 9, true);
    const auto sharedHead =
        builder.addUnaryChain(sharedSource, {"filter", "project", "aggregate-keyed"}, 8, 0, 1, 2 + (size % 3), 7.1);

    const auto firstSideSource = builder.addNode("source", 8, true);
    const auto firstSideBranch = builder.addUnaryChain(firstSideSource, {"map", "enrich"}, 7, 2, 3, 3 + (size % 3), 7.8);
    const auto firstJoin = builder.addNode("join-hash", 4);
    builder.addEdge(firstJoin, sharedHead, builder.hostModulo(2), builder.hostModulo(4), 5 + (size % 2), 12.7);
    builder.addEdge(firstJoin, firstSideBranch, builder.hostModulo(3), builder.hostModulo(4), 5 + ((size + 1) % 2), 12.2);
    const auto firstQueryTail = builder.addNode("aggregate-window", 3);
    builder.addEdge(firstQueryTail, firstJoin, builder.hostModulo(4), sinkHost, 6 + (size % 2), 11.6);
    const auto sinkOne = builder.addNode("sink", 2);
    builder.addEdge(sinkOne, firstQueryTail, sinkHost, sinkHost, 7 + (size % 2), 12.0);

    const auto secondSideSource = builder.addNode("source", 8, true);
    const auto secondSideBranch = builder.addUnaryChain(secondSideSource, {"filter", "project"}, 7, 4, 5, 3 + ((size + 1) % 3), 7.5);
    const auto secondJoin = builder.addNode("join-window", 4);
    builder.addEdge(secondJoin, sharedHead, builder.hostModulo(2), builder.hostModulo(5), 6 + (size % 2), 13.0);
    builder.addEdge(secondJoin, secondSideBranch, builder.hostModulo(5), builder.hostModulo(5), 5 + ((size + 1) % 2), 12.5);
    const auto secondQueryTail = builder.addNode("project", 3);
    builder.addEdge(secondQueryTail, secondJoin, builder.hostModulo(5), sinkHost, 6 + ((size + 1) % 2), 10.8);
    const auto sinkTwo = builder.addNode("sink", 2);
    builder.addEdge(sinkTwo, secondQueryTail, sinkHost, sinkHost, 7 + ((size + 1) % 2), 11.9);

    const auto sinkThree = builder.addNode("sink", 1);
    builder.addEdge(sinkThree, sharedHead, builder.hostModulo(3), sinkHost, 6 + ((size + 2) % 2), 10.7);

    return builder.addSyntheticRoot({sinkOne, sinkTwo, sinkThree});
}

[[nodiscard]] ScenarioData buildDagScenarioTemplate(
    const DagScenarioKind kind,
    const size_t sizeParameter,
    const size_t hostCount,
    const uint64_t seed,
    const BaselineMode baselineMode = BaselineMode::Exact,
    const NetworkTopology networkTopology = NetworkTopology::Line)
{
    DagScenarioBuilder builder(kind, sizeParameter, hostCount, seed, baselineMode, networkTopology);

    size_t rootNodeIndex = 0;
    switch (kind)
    {
        case DagScenarioKind::Diamond:
            rootNodeIndex = buildDiamondScenario(builder);
            break;
        case DagScenarioKind::Join:
            rootNodeIndex = buildJoinScenario(builder);
            break;
        case DagScenarioKind::Pipeline:
            rootNodeIndex = buildPipelineScenario(builder);
            break;
        case DagScenarioKind::Union:
            rootNodeIndex = buildUnionScenario(builder);
            break;
        case DagScenarioKind::MultiJoin:
            rootNodeIndex = buildMultiJoinScenario(builder);
            break;
        case DagScenarioKind::JoinUnionHybrid:
            rootNodeIndex = buildJoinUnionHybridScenario(builder);
            break;
        case DagScenarioKind::SharedSubplan:
            rootNodeIndex = buildSharedSubplanScenario(builder);
            break;
        case DagScenarioKind::BranchingPipeline:
            rootNodeIndex = buildBranchingPipelineScenario(builder);
            break;
        case DagScenarioKind::ReconvergingFanout:
            rootNodeIndex = buildReconvergingFanoutScenario(builder);
            break;
        case DagScenarioKind::MergedPipeline:
            rootNodeIndex = buildMergedPipelineScenario(builder);
            break;
        case DagScenarioKind::MergedUnion:
            rootNodeIndex = buildMergedUnionScenario(builder);
            break;
        case DagScenarioKind::MergedJoin:
            rootNodeIndex = buildMergedJoinScenario(builder);
            break;
    }

    builder.scenario.rootNodeIndex = rootNodeIndex;
    builder.scenario.candidateSet.rootOperatorId = builder.scenario.nodes[rootNodeIndex].id;
    return std::move(builder.scenario);
}

[[nodiscard]] ScenarioData buildTreeScenarioTemplate(const size_t treeHeight, const size_t fanout, const size_t hostCount, const uint64_t seed)
{
    std::mt19937_64 randomEngine(seed);
    ScenarioData scenario;
    scenario.scenarioName = "kary-tree-recording-selection";
    scenario.baselineMode = BaselineMode::Exact;
    initializeScenarioNetwork(scenario, hostCount, NetworkTopology::Line, seed);
    scenario.storageBudgetsBytes.assign(hostCount, 0);

    uint64_t nextOperatorId = 1;
    uint64_t nextRecordingNumber = 0;
    auto buildNode = [&](this auto&& self, const size_t depth, const uint64_t pathOrdinal) -> size_t
    {
        const auto nodeIndex = scenario.nodes.size();
        scenario.nodes.push_back(TreeNode{
            .id = OperatorId(nextOperatorId++),
            .kind = depth == treeHeight ? "source" : "tree",
            .childEdgeIndices = {},
            .replayTimeMs = 0.0,
            .isLeaf = depth == treeHeight});

        const auto subtreeLeafCount = powU64(static_cast<uint64_t>(fanout), treeHeight - depth);
        if (depth == treeHeight)
        {
            scenario.candidateSet.leafOperatorIds.push_back(scenario.nodes[nodeIndex].id);
            return nodeIndex;
        }

        const auto replayTimeJitter = sampleJitter(randomEngine);
        scenario.nodes[nodeIndex].replayTimeMs = std::pow(static_cast<double>(subtreeLeafCount), 1.05) * 1.4 * replayTimeJitter;
        scenario.candidateSet.operatorReplayTimes.push_back(
            RecordingCandidateSet::OperatorReplayTime{.operatorId = scenario.nodes[nodeIndex].id, .replayTimeMs = scenario.nodes[nodeIndex].replayTimeMs});

        for (size_t childOrdinal = 0; childOrdinal < fanout; ++childOrdinal)
        {
            const auto childPathOrdinal = (pathOrdinal * static_cast<uint64_t>(fanout)) + childOrdinal + 1U;
            const auto childIndex = self(depth + 1, childPathOrdinal);
            const auto edge = RecordingPlanEdge{.parentId = scenario.nodes[nodeIndex].id, .childId = scenario.nodes[childIndex].id};
            scenario.candidateSet.planEdges.push_back(edge);

            const auto childSubtreeLeafCount = powU64(static_cast<uint64_t>(fanout), treeHeight - depth - 1);
            const auto maintenanceCostJitter = sampleJitter(randomEngine);
            const auto storageJitter = sampleJitter(randomEngine);
            const auto replayLatencyJitter = sampleJitter(randomEngine);

            const auto maintenanceCost
                = std::pow(static_cast<double>(childSubtreeLeafCount), 1.20) * 12.0 * maintenanceCostJitter;
            const auto storageBytes = static_cast<size_t>(
                std::ceil((256.0 * 1024.0 * static_cast<double>(childSubtreeLeafCount)) + (96.0 * 1024.0 * storageJitter)));
            const auto replayLatencyMs = static_cast<uint64_t>(
                std::ceil((3.0 + (2.5 * static_cast<double>(childSubtreeLeafCount))) * replayLatencyJitter));

            const auto hostIndex = static_cast<size_t>((childPathOrdinal + depth) % hostCount);
            auto option = makeCandidateOption(
                scenario.hosts[hostIndex], childSubtreeLeafCount, maintenanceCost, storageBytes, replayLatencyMs, nextRecordingNumber++);
            auto candidate = RecordingBoundaryCandidate{
                .edge = edge,
                .upstreamNode = scenario.hosts[hostIndex],
                .downstreamNode = scenario.hosts[hostIndex],
                .routeNodes = {scenario.hosts[hostIndex]},
                .materializationEdges = {},
                .activeQueryMaterializationTargets = {},
                .beneficiaryQueries = {},
                .coversIncomingQuery = true,
                .options = {option}};

            scenario.nodes[nodeIndex].childEdgeIndices.push_back(scenario.edges.size());
            scenario.edges.push_back(TreeEdge{
                .edge = edge,
                .parentNodeIndex = nodeIndex,
                .childNodeIndex = childIndex,
                .hostIndex = hostIndex,
                .routeNodes = {scenario.hosts[hostIndex]},
                .candidate = candidate});
            scenario.candidateSet.candidates.push_back(std::move(candidate));
        }

        return nodeIndex;
    };

    scenario.rootNodeIndex = buildNode(0, 0);
    scenario.candidateSet.rootOperatorId = scenario.nodes[scenario.rootNodeIndex].id;
    return scenario;
}

[[nodiscard]] ScenarioData buildScenarioTemplate(
    const BenchmarkConfig& config,
    const size_t treeHeight,
    const size_t fanout,
    const size_t hostCount,
    const uint64_t seed,
    const BaselineMode baselineMode = BaselineMode::Exact)
{
    if (config.scenarioFamily == ScenarioFamily::DagMix)
    {
        return buildDagScenarioTemplate(chooseDagScenarioKind(config, fanout, seed), fanout, hostCount, seed, baselineMode, config.networkTopology);
    }
    return buildTreeScenarioTemplate(treeHeight, fanout, hostCount, seed);
}

[[nodiscard]] ScenarioData buildScenarioTemplate(const FrozenScenario& frozenScenario)
{
    const auto baselineMode = parseBaselineMode(frozenScenario.baselineMode);
    if (frozenScenario.scenario == "kary-tree-recording-selection")
    {
        return buildTreeScenarioTemplate(frozenScenario.treeHeight, frozenScenario.fanout, frozenScenario.hostCount, frozenScenario.seed);
    }
    return buildDagScenarioTemplate(
        parseDagScenarioKind(frozenScenario.scenario),
        frozenScenario.fanout,
        frozenScenario.hostCount,
        frozenScenario.seed,
        baselineMode,
        parseNetworkTopology(frozenScenario.networkTopology));
}

void applyEdgeMetrics(const TreeEdge& edge, CutMetrics& metrics, const bool add)
{
    const auto& option = requireCandidate(edge).options.front();
    if (add)
    {
        metrics.maintenanceCost += option.cost.maintenanceCost;
        metrics.replayTimeMs += static_cast<double>(option.cost.estimatedReplayLatencyMs) * option.cost.replayTimeMultiplier;
        metrics.storageBytesByHost[edge.hostIndex] += option.cost.incrementalStorageBytes;
        ++metrics.selectedEdgeCount;
        return;
    }

    metrics.maintenanceCost -= option.cost.maintenanceCost;
    metrics.replayTimeMs -= static_cast<double>(option.cost.estimatedReplayLatencyMs) * option.cost.replayTimeMultiplier;
    metrics.storageBytesByHost[edge.hostIndex] -= option.cost.incrementalStorageBytes;
    --metrics.selectedEdgeCount;
}

std::vector<CutMetrics> enumerateChildChoices(
    const ScenarioData& scenario, const size_t nodeIndex, const size_t childPosition, CutMetrics currentMetrics);

[[nodiscard]] std::vector<CutMetrics> enumerateNodeCuts(const ScenarioData& scenario, const size_t nodeIndex, CutMetrics currentMetrics)
{
    const auto& node = scenario.nodes[nodeIndex];
    if (node.childEdgeIndices.empty())
    {
        return {};
    }

    currentMetrics.replayTimeMs += node.replayTimeMs;
    return enumerateChildChoices(scenario, nodeIndex, 0, std::move(currentMetrics));
}

[[nodiscard]] std::vector<CutMetrics> enumerateChildChoices(
    const ScenarioData& scenario, const size_t nodeIndex, const size_t childPosition, CutMetrics currentMetrics)
{
    const auto& node = scenario.nodes[nodeIndex];
    if (childPosition == node.childEdgeIndices.size())
    {
        return {std::move(currentMetrics)};
    }

    const auto edgeIndex = node.childEdgeIndices[childPosition];
    const auto& edge = scenario.edges[edgeIndex];
    std::vector<CutMetrics> cuts;

    if (edgeHasCandidate(edge))
    {
        auto cutAtEdgeMetrics = currentMetrics;
        applyEdgeMetrics(edge, cutAtEdgeMetrics, true);
        auto cutAtEdgeCuts = enumerateChildChoices(scenario, nodeIndex, childPosition + 1, std::move(cutAtEdgeMetrics));
        cuts.insert(cuts.end(), std::make_move_iterator(cutAtEdgeCuts.begin()), std::make_move_iterator(cutAtEdgeCuts.end()));
    }

    auto subtreeCuts = enumerateNodeCuts(scenario, edge.childNodeIndex, currentMetrics);
    for (auto& subtreeMetrics : subtreeCuts)
    {
        auto siblingCuts = enumerateChildChoices(scenario, nodeIndex, childPosition + 1, std::move(subtreeMetrics));
        cuts.insert(cuts.end(), std::make_move_iterator(siblingCuts.begin()), std::make_move_iterator(siblingCuts.end()));
    }
    return cuts;
}

[[nodiscard]] std::vector<CutMetrics> enumerateAllTreeCuts(const ScenarioData& scenario)
{
    CutMetrics seedMetrics{
        .maintenanceCost = 0.0,
        .replayTimeMs = 0.0,
        .storageBytesByHost = std::vector<size_t>(scenario.hosts.size(), 0),
        .selectedEdgeCount = 0};
    return enumerateNodeCuts(scenario, scenario.rootNodeIndex, std::move(seedMetrics));
}

[[nodiscard]] size_t findNodeIndexById(const ScenarioData& scenario, const OperatorId operatorId)
{
    const auto it = std::ranges::find(scenario.nodes, operatorId, &TreeNode::id);
    if (it == scenario.nodes.end())
    {
        throw std::runtime_error("scenario references an unknown operator");
    }
    return static_cast<size_t>(std::distance(scenario.nodes.begin(), it));
}

[[nodiscard]] std::vector<CutMetrics> enumerateAllGenericDagCuts(const ScenarioData& scenario)
{
    std::vector<size_t> leafNodeIndices;
    leafNodeIndices.reserve(scenario.candidateSet.leafOperatorIds.size());
    for (const auto leafOperatorId : scenario.candidateSet.leafOperatorIds)
    {
        leafNodeIndices.push_back(findNodeIndexById(scenario, leafOperatorId));
    }

    std::vector<size_t> variableNodeIndices;
    variableNodeIndices.reserve(scenario.nodes.size());
    for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
    {
        if (nodeIndex == scenario.rootNodeIndex || scenario.nodes[nodeIndex].isLeaf)
        {
            continue;
        }
        variableNodeIndices.push_back(nodeIndex);
    }
    if (variableNodeIndices.size() >= 20)
    {
        throw std::runtime_error("generic DAG exact enumeration exceeded the supported variable-node limit");
    }

    std::vector<CutMetrics> cuts;
    const auto totalMasks = uint64_t{1} << variableNodeIndices.size();
    for (uint64_t mask = 0; mask < totalMasks; ++mask)
    {
        std::vector<bool> recordedSide(scenario.nodes.size(), false);
        for (const auto leafNodeIndex : leafNodeIndices)
        {
            recordedSide[leafNodeIndex] = true;
        }
        for (size_t bitIndex = 0; bitIndex < variableNodeIndices.size(); ++bitIndex)
        {
            recordedSide[variableNodeIndices[bitIndex]] = ((mask >> bitIndex) & 1U) != 0;
        }
        recordedSide[scenario.rootNodeIndex] = false;

        CutMetrics baseMetrics{
            .maintenanceCost = 0.0,
            .replayTimeMs = 0.0,
            .storageBytesByHost = std::vector<size_t>(scenario.hosts.size(), 0),
            .selectedEdgeCount = 0};
        std::vector<size_t> boundaryEdgeIndices;
        boundaryEdgeIndices.reserve(scenario.edges.size());
        bool usesNonCandidateBoundary = false;
        for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
        {
            if (!recordedSide[nodeIndex])
            {
                baseMetrics.replayTimeMs += scenario.nodes[nodeIndex].replayTimeMs;
            }
        }
        for (size_t edgeIndex = 0; edgeIndex < scenario.edges.size(); ++edgeIndex)
        {
            const auto& edge = scenario.edges[edgeIndex];
            if (recordedSide[edge.childNodeIndex] && !recordedSide[edge.parentNodeIndex])
            {
                if (!edgeHasCandidate(edge))
                {
                    usesNonCandidateBoundary = true;
                    break;
                }
                boundaryEdgeIndices.push_back(edgeIndex);
            }
        }
        if (usesNonCandidateBoundary || boundaryEdgeIndices.empty())
        {
            continue;
        }

        auto currentMetrics = baseMetrics;
        const auto enumerateOptions = [&](this auto&& self, const size_t boundaryPosition) -> void
        {
            if (boundaryPosition == boundaryEdgeIndices.size())
            {
                cuts.push_back(currentMetrics);
                return;
            }

            const auto& candidate = requireCandidate(scenario.edges[boundaryEdgeIndices[boundaryPosition]]);
            for (const auto& option : candidate.options)
            {
                if (!option.feasible)
                {
                    continue;
                }
                applySelectedOptionMetrics(scenario, option, currentMetrics, true);
                self(boundaryPosition + 1);
                applySelectedOptionMetrics(scenario, option, currentMetrics, false);
            }
        };
        enumerateOptions(0);
    }
    return cuts;
}

[[nodiscard]] std::vector<CutMetrics> enumerateAllCuts(const ScenarioData& scenario)
{
    return scenario.usesGenericExactEnumeration ? enumerateAllGenericDagCuts(scenario) : enumerateAllTreeCuts(scenario);
}

[[nodiscard]] CandidateOptionSummary summarizeCandidateOptions(const ScenarioData& scenario)
{
    CandidateOptionSummary summary{
        .candidateEdges = scenario.candidateSet.candidates.size(),
        .candidateOptions = 0,
        .reuseOptions = 0,
        .upgradeOptions = 0,
        .createOptions = 0};
    for (const auto& candidate : scenario.candidateSet.candidates)
    {
        summary.candidateOptions += candidate.options.size();
        for (const auto& option : candidate.options)
        {
            switch (option.decision)
            {
                case RecordingSelectionDecision::ReuseExistingRecording:
                    ++summary.reuseOptions;
                    break;
                case RecordingSelectionDecision::UpgradeExistingRecording:
                    ++summary.upgradeOptions;
                    break;
                case RecordingSelectionDecision::CreateNewRecording:
                    ++summary.createOptions;
                    break;
            }
        }
    }
    return summary;
}

[[nodiscard]] std::vector<size_t> maximumScenarioStorageByHost(const ScenarioData& scenario)
{
    std::vector<size_t> storageBytesByHost(scenario.hosts.size(), 0);
    for (const auto& candidate : scenario.candidateSet.candidates)
    {
        std::vector<size_t> perCandidateHostMax(scenario.hosts.size(), 0);
        for (const auto& option : candidate.options)
        {
            if (!option.feasible || option.decision == RecordingSelectionDecision::ReuseExistingRecording)
            {
                continue;
            }

            const auto hostIndex = findHostIndex(scenario.hosts, option.selection.node);
            perCandidateHostMax[hostIndex] = std::max(perCandidateHostMax[hostIndex], option.cost.incrementalStorageBytes);
        }
        for (size_t hostIndex = 0; hostIndex < storageBytesByHost.size(); ++hostIndex)
        {
            storageBytesByHost[hostIndex] += perCandidateHostMax[hostIndex];
        }
    }
    return storageBytesByHost;
}

template <class T>
[[nodiscard]] T quantile(std::vector<T> values, const double q)
{
    if (values.empty())
    {
        throw std::invalid_argument("quantile requires at least one value");
    }

    std::ranges::sort(values);
    const auto position = static_cast<size_t>(std::llround(q * static_cast<double>(values.size() - 1)));
    return values.at(position);
}

[[nodiscard]] bool isFeasibleCut(const CutMetrics& metrics, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    if (metrics.replayTimeMs > static_cast<double>(replayLatencyLimitMs) + EPSILON)
    {
        return false;
    }
    for (size_t hostIndex = 0; hostIndex < storageBudgetsBytes.size(); ++hostIndex)
    {
        if (metrics.storageBytesByHost[hostIndex] > storageBudgetsBytes[hostIndex])
        {
            return false;
        }
    }
    return true;
}

void applyBudgetQuantiles(const std::vector<CutMetrics>& allCuts, const double latencyQuantile, const double storageQuantile, ScenarioData& scenario)
{
    std::vector<double> replayTimes;
    replayTimes.reserve(allCuts.size());
    for (const auto& cut : allCuts)
    {
        replayTimes.push_back(cut.replayTimeMs);
    }
    scenario.replayLatencyLimitMs = static_cast<uint64_t>(std::ceil(quantile(replayTimes, latencyQuantile)));

    for (size_t hostIndex = 0; hostIndex < scenario.hosts.size(); ++hostIndex)
    {
        std::vector<size_t> hostBytes;
        hostBytes.reserve(allCuts.size());
        for (const auto& cut : allCuts)
        {
            hostBytes.push_back(cut.storageBytesByHost[hostIndex]);
        }
        scenario.storageBudgetsBytes[hostIndex] = std::max<size_t>(1, quantile(hostBytes, storageQuantile));
    }
    scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;
}

void deriveScenarioBudgets(
    const std::vector<CutMetrics>& allCuts,
    const double baseLatencyQuantile,
    const double baseStorageQuantile,
    const size_t minFeasibleCutTarget,
    ScenarioData& scenario,
    double& latencyQuantileValue,
    double& storageQuantileValue)
{
    latencyQuantileValue = baseLatencyQuantile;
    storageQuantileValue = baseStorageQuantile;

    while (true)
    {
        applyBudgetQuantiles(allCuts, latencyQuantileValue, storageQuantileValue, scenario);
        const auto feasibleCutCount = static_cast<size_t>(std::ranges::count_if(
            allCuts,
            [&](const CutMetrics& cut) { return isFeasibleCut(cut, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes); }));
        if (feasibleCutCount >= minFeasibleCutTarget || (latencyQuantileValue >= 1.0 && storageQuantileValue >= 1.0))
        {
            return;
        }

        latencyQuantileValue = std::min(1.0, latencyQuantileValue + 0.05);
        storageQuantileValue = std::min(1.0, storageQuantileValue + 0.05);
    }
}

[[nodiscard]] SharedPtr<WorkerCatalog> makeWorkerCatalog(const ScenarioData& scenario)
{
    auto workerCatalog = std::make_shared<WorkerCatalog>();
    for (size_t hostIndex = 0; hostIndex < scenario.hosts.size(); ++hostIndex)
    {
        std::vector<Host> downstream;
        if (scenario.hostNeighborsByIndex.size() == scenario.hosts.size())
        {
            for (const auto neighborIndex : scenario.hostNeighborsByIndex[hostIndex])
            {
                if (neighborIndex > hostIndex)
                {
                    downstream.push_back(scenario.hosts[neighborIndex]);
                }
            }
        }
        else if (hostIndex + 1 < scenario.hosts.size())
        {
            downstream.push_back(scenario.hosts[hostIndex + 1]);
        }
        if (!workerCatalog->addWorker(scenario.hosts[hostIndex], {}, 16, downstream, {}, scenario.storageBudgetsBytes[hostIndex]))
        {
            throw std::runtime_error("failed to create synthetic worker catalog");
        }
    }
    return workerCatalog;
}

[[nodiscard]] std::shared_ptr<const WorkerCatalog> makeConstWorkerCatalog(const SharedPtr<WorkerCatalog>& workerCatalog)
{
    return copyPtr(workerCatalog);
}

[[nodiscard]] ScenarioStats computeFeasibleCutStats(
    const std::vector<CutMetrics>& allCuts, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    ScenarioStats stats{
        .validCutCount = allCuts.size(),
        .feasibleCutCount = 0,
        .bestMaintenanceCost = 0.0,
        .bestReplayTimeMs = 0.0,
        .averageMaintenanceCost = 0.0,
        .worstMaintenanceCost = 0.0};
    auto bestMaintenanceCost = std::numeric_limits<double>::infinity();
    auto bestReplayTimeMs = std::numeric_limits<double>::infinity();
    auto worstMaintenanceCost = 0.0;
    double maintenanceCostSum = 0.0;
    size_t feasibleCutCount = 0;

    for (const auto& cut : allCuts)
    {
        if (!isFeasibleCut(cut, replayLatencyLimitMs, storageBudgetsBytes))
        {
            continue;
        }

        ++feasibleCutCount;
        maintenanceCostSum += cut.maintenanceCost;
        worstMaintenanceCost = std::max(worstMaintenanceCost, cut.maintenanceCost);
        if (cut.maintenanceCost < bestMaintenanceCost - EPSILON
            || (std::abs(cut.maintenanceCost - bestMaintenanceCost) <= EPSILON && cut.replayTimeMs < bestReplayTimeMs - EPSILON))
        {
            bestMaintenanceCost = cut.maintenanceCost;
            bestReplayTimeMs = cut.replayTimeMs;
        }
    }

    if (feasibleCutCount == 0)
    {
        throw std::runtime_error("benchmark scenario did not yield any feasible cuts");
    }

    stats.feasibleCutCount = feasibleCutCount;
    stats.bestMaintenanceCost = bestMaintenanceCost;
    stats.bestReplayTimeMs = bestReplayTimeMs;
    stats.averageMaintenanceCost = maintenanceCostSum / static_cast<double>(feasibleCutCount);
    stats.worstMaintenanceCost = worstMaintenanceCost;
    return stats;
}

[[nodiscard]] CutMetrics computeUnconstrainedBestCut(const std::vector<CutMetrics>& allCuts)
{
    if (allCuts.empty())
    {
        throw std::runtime_error("benchmark scenario did not yield any valid cuts");
    }

    auto bestCut = allCuts.front();
    for (const auto& cut : allCuts)
    {
        if (cut.maintenanceCost < bestCut.maintenanceCost - EPSILON
            || (std::abs(cut.maintenanceCost - bestCut.maintenanceCost) <= EPSILON && cut.replayTimeMs < bestCut.replayTimeMs - EPSILON))
        {
            bestCut = cut;
        }
    }
    return bestCut;
}

[[nodiscard]] bool betterCutMetrics(const CutMetrics& lhs, const CutMetrics& rhs)
{
    return lhs.maintenanceCost < rhs.maintenanceCost - EPSILON
        || (std::abs(lhs.maintenanceCost - rhs.maintenanceCost) <= EPSILON && lhs.replayTimeMs < rhs.replayTimeMs - EPSILON);
}

std::vector<SelectionWithMetrics> enumerateTreeChildSelections(
    const ScenarioData& scenario, const size_t nodeIndex, const size_t childPosition, SelectionWithMetrics currentSelection);

[[nodiscard]] std::vector<SelectionWithMetrics> enumerateTreeNodeSelections(
    const ScenarioData& scenario, const size_t nodeIndex, SelectionWithMetrics currentSelection)
{
    const auto& node = scenario.nodes[nodeIndex];
    if (node.childEdgeIndices.empty())
    {
        return {};
    }

    currentSelection.metrics.replayTimeMs += node.replayTimeMs;
    return enumerateTreeChildSelections(scenario, nodeIndex, 0, std::move(currentSelection));
}

[[nodiscard]] std::vector<SelectionWithMetrics> enumerateTreeChildSelections(
    const ScenarioData& scenario, const size_t nodeIndex, const size_t childPosition, SelectionWithMetrics currentSelection)
{
    const auto& node = scenario.nodes[nodeIndex];
    if (childPosition == node.childEdgeIndices.size())
    {
        return {std::move(currentSelection)};
    }

    const auto edgeIndex = node.childEdgeIndices[childPosition];
    const auto& edge = scenario.edges[edgeIndex];
    std::vector<SelectionWithMetrics> selections;

    if (edgeHasCandidate(edge))
    {
        const auto& candidate = requireCandidate(edge);
        auto cutAtEdgeSelection = currentSelection;
        applyEdgeMetrics(edge, cutAtEdgeSelection.metrics, true);
        cutAtEdgeSelection.selection.selectedBoundary.push_back(
            SelectedRecordingBoundary{.candidate = candidate, .chosenOption = candidate.options.front(), .alternatives = {}});
        auto cutAtEdgeSelections = enumerateTreeChildSelections(scenario, nodeIndex, childPosition + 1, std::move(cutAtEdgeSelection));
        selections.insert(
            selections.end(), std::make_move_iterator(cutAtEdgeSelections.begin()), std::make_move_iterator(cutAtEdgeSelections.end()));
    }

    auto subtreeSelections = enumerateTreeNodeSelections(scenario, edge.childNodeIndex, currentSelection);
    for (auto& subtreeSelection : subtreeSelections)
    {
        auto siblingSelections = enumerateTreeChildSelections(scenario, nodeIndex, childPosition + 1, std::move(subtreeSelection));
        selections.insert(
            selections.end(), std::make_move_iterator(siblingSelections.begin()), std::make_move_iterator(siblingSelections.end()));
    }
    return selections;
}

[[nodiscard]] std::vector<SelectionWithMetrics> enumerateAllTreeSelections(const ScenarioData& scenario)
{
    SelectionWithMetrics seedSelection{
        .selection = RecordingBoundarySelection{},
        .metrics = CutMetrics{
            .maintenanceCost = 0.0,
            .replayTimeMs = 0.0,
            .storageBytesByHost = std::vector<size_t>(scenario.hosts.size(), 0),
            .selectedEdgeCount = 0}};
    return enumerateTreeNodeSelections(scenario, scenario.rootNodeIndex, std::move(seedSelection));
}

[[nodiscard]] SelectionWithMetrics computeBestExactTreeSelection(
    const ScenarioData& scenario, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    std::optional<SelectionWithMetrics> bestSelection;
    for (auto& candidateSelection : enumerateAllTreeSelections(scenario))
    {
        if (!isFeasibleCut(candidateSelection.metrics, replayLatencyLimitMs, storageBudgetsBytes))
        {
            continue;
        }
        if (!bestSelection.has_value() || betterCutMetrics(candidateSelection.metrics, bestSelection->metrics))
        {
            candidateSelection.selection.objectiveCost = candidateSelection.metrics.maintenanceCost;
            bestSelection = std::move(candidateSelection);
        }
    }

    if (!bestSelection.has_value())
    {
        throw std::runtime_error("exact tree baseline did not yield any feasible selection");
    }
    return *bestSelection;
}

[[nodiscard]] SelectionWithMetrics computeBestExactGenericDagSelection(
    const ScenarioData& scenario, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    std::vector<size_t> leafNodeIndices;
    leafNodeIndices.reserve(scenario.candidateSet.leafOperatorIds.size());
    for (const auto leafOperatorId : scenario.candidateSet.leafOperatorIds)
    {
        leafNodeIndices.push_back(findNodeIndexById(scenario, leafOperatorId));
    }

    std::vector<size_t> variableNodeIndices;
    variableNodeIndices.reserve(scenario.nodes.size());
    for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
    {
        if (nodeIndex == scenario.rootNodeIndex || scenario.nodes[nodeIndex].isLeaf)
        {
            continue;
        }
        variableNodeIndices.push_back(nodeIndex);
    }
    if (variableNodeIndices.size() >= 20)
    {
        throw std::runtime_error("generic DAG exact enumeration exceeded the supported variable-node limit");
    }

    std::optional<SelectionWithMetrics> bestSelection;
    const auto totalMasks = uint64_t{1} << variableNodeIndices.size();
    for (uint64_t mask = 0; mask < totalMasks; ++mask)
    {
        std::vector<bool> recordedSide(scenario.nodes.size(), false);
        for (const auto leafNodeIndex : leafNodeIndices)
        {
            recordedSide[leafNodeIndex] = true;
        }
        for (size_t bitIndex = 0; bitIndex < variableNodeIndices.size(); ++bitIndex)
        {
            recordedSide[variableNodeIndices[bitIndex]] = ((mask >> bitIndex) & 1U) != 0;
        }
        recordedSide[scenario.rootNodeIndex] = false;

        SelectionWithMetrics currentSelection{
            .selection = RecordingBoundarySelection{},
            .metrics = CutMetrics{
                .maintenanceCost = 0.0,
                .replayTimeMs = 0.0,
                .storageBytesByHost = std::vector<size_t>(scenario.hosts.size(), 0),
                .selectedEdgeCount = 0}};
        std::vector<size_t> boundaryEdgeIndices;
        boundaryEdgeIndices.reserve(scenario.edges.size());
        bool usesNonCandidateBoundary = false;
        for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
        {
            if (!recordedSide[nodeIndex])
            {
                currentSelection.metrics.replayTimeMs += scenario.nodes[nodeIndex].replayTimeMs;
            }
        }
        for (size_t edgeIndex = 0; edgeIndex < scenario.edges.size(); ++edgeIndex)
        {
            const auto& edge = scenario.edges[edgeIndex];
            if (recordedSide[edge.childNodeIndex] && !recordedSide[edge.parentNodeIndex])
            {
                if (!edgeHasCandidate(edge))
                {
                    usesNonCandidateBoundary = true;
                    break;
                }
                boundaryEdgeIndices.push_back(edgeIndex);
            }
        }
        if (usesNonCandidateBoundary || boundaryEdgeIndices.empty())
        {
            continue;
        }

        currentSelection.selection.selectedBoundary.reserve(boundaryEdgeIndices.size());
        const auto enumerateOptions = [&](this auto&& self, const size_t boundaryPosition) -> void
        {
            if (boundaryPosition == boundaryEdgeIndices.size())
            {
                if (!isFeasibleCut(currentSelection.metrics, replayLatencyLimitMs, storageBudgetsBytes))
                {
                    return;
                }
                if (!bestSelection.has_value() || betterCutMetrics(currentSelection.metrics, bestSelection->metrics))
                {
                    auto candidateSelection = currentSelection;
                    candidateSelection.selection.objectiveCost = candidateSelection.metrics.maintenanceCost;
                    bestSelection = std::move(candidateSelection);
                }
                return;
            }

            const auto edgeIndex = boundaryEdgeIndices[boundaryPosition];
            const auto& edge = scenario.edges[edgeIndex];
            const auto& candidate = requireCandidate(edge);
            for (const auto& option : candidate.options)
            {
                if (!option.feasible)
                {
                    continue;
                }
                applySelectedOptionMetrics(scenario, option, currentSelection.metrics, true);
                currentSelection.selection.selectedBoundary.push_back(
                    SelectedRecordingBoundary{.candidate = candidate, .chosenOption = option, .alternatives = {}});
                self(boundaryPosition + 1);
                currentSelection.selection.selectedBoundary.pop_back();
                applySelectedOptionMetrics(scenario, option, currentSelection.metrics, false);
            }
        };
        enumerateOptions(0);
    }

    if (!bestSelection.has_value())
    {
        throw std::runtime_error("exact DAG baseline did not yield any feasible selection");
    }
    return *bestSelection;
}

[[nodiscard]] SelectionWithMetrics computeBestExactSelection(
    const ScenarioData& scenario, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    return scenario.usesGenericExactEnumeration
        ? computeBestExactGenericDagSelection(scenario, replayLatencyLimitMs, storageBudgetsBytes)
        : computeBestExactTreeSelection(scenario, replayLatencyLimitMs, storageBudgetsBytes);
}

[[nodiscard]] CutMetrics evaluateSelectionMetrics(
    const ScenarioData& scenario,
    const RecordingBoundarySelection& selection,
    size_t& visitedSelectedEdges)
{
    std::unordered_set<RecordingPlanEdge, RecordingPlanEdgeHash> selectedEdges;
    selectedEdges.reserve(selection.selectedBoundary.size());
    std::unordered_map<RecordingPlanEdge, const RecordingCandidateOption*, RecordingPlanEdgeHash> chosenOptionsByEdge;
    chosenOptionsByEdge.reserve(selection.selectedBoundary.size());
    for (const auto& selectedBoundary : selection.selectedBoundary)
    {
        selectedEdges.insert(selectedBoundary.candidate.edge);
        chosenOptionsByEdge.emplace(selectedBoundary.candidate.edge, &selectedBoundary.chosenOption);
    }

    CutMetrics metrics{
        .maintenanceCost = 0.0,
        .replayTimeMs = 0.0,
        .storageBytesByHost = std::vector<size_t>(scenario.hosts.size(), 0),
        .selectedEdgeCount = 0};
    std::vector<bool> recordedSide(scenario.nodes.size(), false);
    std::vector<size_t> stack;
    stack.reserve(scenario.nodes.size());
    for (const auto leafOperatorId : scenario.candidateSet.leafOperatorIds)
    {
        const auto leafNodeIndex = findNodeIndexById(scenario, leafOperatorId);
        if (!recordedSide[leafNodeIndex])
        {
            recordedSide[leafNodeIndex] = true;
            stack.push_back(leafNodeIndex);
        }
    }

    while (!stack.empty())
    {
        const auto nodeIndex = stack.back();
        stack.pop_back();
        for (const auto& edge : scenario.edges)
        {
            if (edge.childNodeIndex != nodeIndex || selectedEdges.contains(edge.edge) || recordedSide[edge.parentNodeIndex])
            {
                continue;
            }
            recordedSide[edge.parentNodeIndex] = true;
            stack.push_back(edge.parentNodeIndex);
        }
    }

    if (recordedSide[scenario.rootNodeIndex])
    {
        throw std::runtime_error("solver returned a non-boundary cut for the synthetic scenario");
    }

    for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
    {
        if (!recordedSide[nodeIndex])
        {
            metrics.replayTimeMs += scenario.nodes[nodeIndex].replayTimeMs;
        }
    }

    visitedSelectedEdges = 0;
    for (const auto& edge : scenario.edges)
    {
        const auto childRecorded = recordedSide[edge.childNodeIndex];
        const auto parentRecorded = recordedSide[edge.parentNodeIndex];
        if (childRecorded && !parentRecorded)
        {
            if (!edgeHasCandidate(edge) || !selectedEdges.contains(edge.edge) || !chosenOptionsByEdge.contains(edge.edge))
            {
                throw std::runtime_error("solver returned a non-boundary cut for the synthetic scenario");
            }
            ++visitedSelectedEdges;
            applySelectedOptionMetrics(scenario, *chosenOptionsByEdge.at(edge.edge), metrics, true);
            continue;
        }

        if (selectedEdges.contains(edge.edge))
        {
            throw std::runtime_error("solver returned a non-boundary cut for the synthetic scenario");
        }
    }
    return metrics;
}

[[nodiscard]] size_t computeSelectionRank(
    const CutMetrics& selectedMetrics, const std::vector<CutMetrics>& allCuts, const uint64_t replayLatencyLimitMs, const std::vector<size_t>& storageBudgetsBytes)
{
    size_t betterCuts = 0;
    for (const auto& cut : allCuts)
    {
        if (!isFeasibleCut(cut, replayLatencyLimitMs, storageBudgetsBytes))
        {
            continue;
        }
        if (cut.maintenanceCost < selectedMetrics.maintenanceCost - EPSILON
            || (std::abs(cut.maintenanceCost - selectedMetrics.maintenanceCost) <= EPSILON
                && cut.replayTimeMs < selectedMetrics.replayTimeMs - EPSILON))
        {
            ++betterCuts;
        }
    }
    return betterCuts + 1;
}

[[nodiscard]] double measureSolverTimeNsMean(
    const RecordingCandidateSet& candidateSet,
    const std::shared_ptr<const WorkerCatalog>& workerCatalog,
    const size_t repetitions,
    const ShadowPriceTuning& shadowPriceTuning = {})
{
    const auto solver = RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{workerCatalog}, shadowPriceTuning);
    const auto start = std::chrono::steady_clock::now();
    for (size_t repetition = 0; repetition < repetitions; ++repetition)
    {
        static_cast<void>(solver.solve(candidateSet));
    }
    const auto end = std::chrono::steady_clock::now();
    const auto totalNs = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    return totalNs / static_cast<double>(repetitions);
}

[[nodiscard]] SolverInvocationResult runSolverOnce(
    const RecordingCandidateSet& candidateSet,
    const std::shared_ptr<const WorkerCatalog>& workerCatalog,
    const ShadowPriceTuning& shadowPriceTuning = {})
{
    const auto start = std::chrono::steady_clock::now();
    try
    {
        auto selection = RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{workerCatalog}, shadowPriceTuning).solve(candidateSet);
        const auto end = std::chrono::steady_clock::now();
        return SolverInvocationResult{
            .selection = std::move(selection),
            .elapsedNs = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()),
            .succeeded = true};
    }
    catch (const std::exception&)
    {
        const auto end = std::chrono::steady_clock::now();
        return SolverInvocationResult{
            .selection = std::nullopt,
            .elapsedNs = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count()),
            .succeeded = false};
    }
}

[[nodiscard]] bool solverCanSolveScenario(const ScenarioData& scenario, const ShadowPriceTuning& shadowPriceTuning = {})
{
    const auto workerCatalog = makeWorkerCatalog(scenario);
    const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
    return runSolverOnce(scenario.candidateSet, constWorkerCatalog, shadowPriceTuning).succeeded;
}

[[nodiscard]] std::string buildInstanceId(
    const size_t treeHeight, const size_t fanout, const uint64_t seed, const std::string_view suffix)
{
    return "h" + std::to_string(treeHeight) + "-f" + std::to_string(fanout) + "-s" + std::to_string(seed) + "-" + std::string(suffix);
}

[[nodiscard]] std::string buildInstanceId(
    const ScenarioData& scenario, const size_t treeHeight, const size_t fanout, const uint64_t seed, const std::string_view suffix)
{
    if (scenario.scenarioName == "kary-tree-recording-selection")
    {
        return buildInstanceId(treeHeight, fanout, seed, suffix);
    }

    auto scenarioPrefix = scenario.scenarioName;
    std::ranges::replace(scenarioPrefix, '-', '_');
    return scenarioPrefix + "-f" + std::to_string(fanout) + "-s" + std::to_string(seed) + "-" + std::string(suffix);
}

[[nodiscard]] std::vector<CorpusBucket> defaultCorpusBuckets()
{
    return {
        CorpusBucket{
            .name = "tight",
            .minFeasibleFraction = 0.01,
            .maxFeasibleFraction = 0.05,
            .attempts = {
                BudgetAttempt{.profileName = "tight-q05", .latencyQuantile = 0.05, .storageQuantile = 0.05},
                BudgetAttempt{.profileName = "tight-q10", .latencyQuantile = 0.10, .storageQuantile = 0.10},
                BudgetAttempt{.profileName = "tight-q15", .latencyQuantile = 0.15, .storageQuantile = 0.15},
                BudgetAttempt{.profileName = "tight-q20", .latencyQuantile = 0.20, .storageQuantile = 0.20}}},
        CorpusBucket{
            .name = "medium",
            .minFeasibleFraction = 0.05,
            .maxFeasibleFraction = 0.20,
            .attempts = {
                BudgetAttempt{.profileName = "medium-q20", .latencyQuantile = 0.20, .storageQuantile = 0.20},
                BudgetAttempt{.profileName = "medium-q25", .latencyQuantile = 0.25, .storageQuantile = 0.25},
                BudgetAttempt{.profileName = "medium-q30", .latencyQuantile = 0.30, .storageQuantile = 0.30},
                BudgetAttempt{.profileName = "medium-q35", .latencyQuantile = 0.35, .storageQuantile = 0.35}}},
        CorpusBucket{
            .name = "loose",
            .minFeasibleFraction = 0.20,
            .maxFeasibleFraction = 0.50,
            .attempts = {
                BudgetAttempt{.profileName = "loose-q35", .latencyQuantile = 0.35, .storageQuantile = 0.35},
                BudgetAttempt{.profileName = "loose-q40", .latencyQuantile = 0.40, .storageQuantile = 0.40},
                BudgetAttempt{.profileName = "loose-q45", .latencyQuantile = 0.45, .storageQuantile = 0.45},
                BudgetAttempt{.profileName = "loose-q50", .latencyQuantile = 0.50, .storageQuantile = 0.50}}},
        CorpusBucket{
            .name = "wide",
            .minFeasibleFraction = 0.50,
            .maxFeasibleFraction = 0.95,
            .attempts = {
                BudgetAttempt{.profileName = "wide-q55", .latencyQuantile = 0.55, .storageQuantile = 0.55},
                BudgetAttempt{.profileName = "wide-q60", .latencyQuantile = 0.60, .storageQuantile = 0.60},
                BudgetAttempt{.profileName = "wide-q65", .latencyQuantile = 0.65, .storageQuantile = 0.65},
                BudgetAttempt{.profileName = "wide-q70", .latencyQuantile = 0.70, .storageQuantile = 0.70},
                BudgetAttempt{.profileName = "wide-q75", .latencyQuantile = 0.75, .storageQuantile = 0.75},
                BudgetAttempt{.profileName = "wide-q80", .latencyQuantile = 0.80, .storageQuantile = 0.80},
                BudgetAttempt{.profileName = "wide-q85", .latencyQuantile = 0.85, .storageQuantile = 0.85},
                BudgetAttempt{.profileName = "wide-q90", .latencyQuantile = 0.90, .storageQuantile = 0.90},
                BudgetAttempt{.profileName = "wide-q95", .latencyQuantile = 0.95, .storageQuantile = 0.95},
                BudgetAttempt{.profileName = "wide-q100", .latencyQuantile = 1.0, .storageQuantile = 1.0}}}};
}

[[nodiscard]] double feasibleFraction(const size_t feasibleCutCount, const size_t validCutCount)
{
    return validCutCount == 0 ? 0.0 : static_cast<double>(feasibleCutCount) / static_cast<double>(validCutCount);
}

[[nodiscard]] double bucketMidpoint(const CorpusBucket& bucket)
{
    return (bucket.minFeasibleFraction + bucket.maxFeasibleFraction) * 0.5;
}

[[nodiscard]] std::vector<BudgetAttempt> collectBudgetAttempts(const std::vector<CorpusBucket>& buckets)
{
    std::vector<BudgetAttempt> attempts;
    for (const auto& bucket : buckets)
    {
        attempts.insert(attempts.end(), bucket.attempts.begin(), bucket.attempts.end());
    }
    return attempts;
}

[[nodiscard]] std::vector<PerformanceBudgetAttempt> defaultPerformanceBudgetAttempts()
{
    return {
        PerformanceBudgetAttempt{
            .bucketName = "performance-tight",
            .profileName = "performance-tight-q25",
            .latencyQuantile = 0.25,
            .storageQuantile = 0.25},
        PerformanceBudgetAttempt{
            .bucketName = "performance-medium",
            .profileName = "performance-medium-q45",
            .latencyQuantile = 0.45,
            .storageQuantile = 0.45},
        PerformanceBudgetAttempt{
            .bucketName = "performance-wide",
            .profileName = "performance-wide-q70",
            .latencyQuantile = 0.70,
            .storageQuantile = 0.70},
        PerformanceBudgetAttempt{
            .bucketName = "performance-relaxed",
            .profileName = "performance-relaxed-q85",
            .latencyQuantile = 0.85,
            .storageQuantile = 0.85}};
}

[[nodiscard]] ScenarioData buildRelaxedPerformanceReferenceScenario(const ScenarioData& baseScenario)
{
    auto relaxedScenario = baseScenario;
    relaxedScenario.storageBudgetsBytes = maximumScenarioStorageByHost(baseScenario);
    for (auto& storageBudgetBytes : relaxedScenario.storageBudgetsBytes)
    {
        storageBudgetBytes = std::max<size_t>(storageBudgetBytes, 1);
    }
    relaxedScenario.replayLatencyLimitMs = std::numeric_limits<uint64_t>::max() / 8U;
    relaxedScenario.candidateSet.replayLatencyLimitMs = relaxedScenario.replayLatencyLimitMs;
    return relaxedScenario;
}

[[nodiscard]] std::optional<CutMetrics> trySolveScenarioMetrics(
    const ScenarioData& scenario, const std::shared_ptr<const WorkerCatalog>& workerCatalog)
{
    const auto solverResult = runSolverOnce(scenario.candidateSet, workerCatalog);
    if (!solverResult.succeeded || !solverResult.selection.has_value())
    {
        return std::nullopt;
    }

    size_t visitedSelectedEdges = 0;
    const auto metrics = evaluateSelectionMetrics(scenario, *solverResult.selection, visitedSelectedEdges);
    if (visitedSelectedEdges != solverResult.selection->selectedBoundary.size())
    {
        throw std::runtime_error("performance-only reference selection is not a boundary cut");
    }
    return metrics;
}

[[nodiscard]] PerformanceReferenceSet computePerformanceReferenceMetrics(const ScenarioData& baseScenario)
{
    auto referenceScenario = buildRelaxedPerformanceReferenceScenario(baseScenario);
    const auto workerCatalog = makeWorkerCatalog(referenceScenario);
    const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
    const auto unconstrainedReference = trySolveScenarioMetrics(referenceScenario, constWorkerCatalog);
    if (!unconstrainedReference.has_value())
    {
        throw std::runtime_error("failed to compute relaxed performance-only reference selection");
    }

    auto lowerBoundScenario = referenceScenario;
    auto low = uint64_t{1};
    auto high = std::max<uint64_t>(1, static_cast<uint64_t>(std::ceil(unconstrainedReference->replayTimeMs)));
    std::optional<CutMetrics> replayTightReference = *unconstrainedReference;
    while (low < high)
    {
        const auto mid = low + ((high - low) / 2U);
        lowerBoundScenario.replayLatencyLimitMs = mid;
        lowerBoundScenario.candidateSet.replayLatencyLimitMs = mid;
        if (const auto metrics = trySolveScenarioMetrics(lowerBoundScenario, constWorkerCatalog); metrics.has_value())
        {
            replayTightReference = *metrics;
            high = mid;
        }
        else
        {
            low = mid + 1U;
        }
    }
    lowerBoundScenario.replayLatencyLimitMs = high;
    lowerBoundScenario.candidateSet.replayLatencyLimitMs = high;
    if (const auto metrics = trySolveScenarioMetrics(lowerBoundScenario, constWorkerCatalog); metrics.has_value())
    {
        replayTightReference = *metrics;
    }
    if (!replayTightReference.has_value())
    {
        throw std::runtime_error("failed to compute replay-tight performance-only reference selection");
    }

    return PerformanceReferenceSet{
        .unconstrainedReference = *unconstrainedReference,
        .replayTightReference = *replayTightReference,
        .maxStorageByHost = maximumScenarioStorageByHost(baseScenario)};
}

void applyHeuristicPerformanceBudgets(
    const PerformanceReferenceSet& referenceSet,
    const double latencyQuantile,
    const double storageQuantile,
    ScenarioData& scenario)
{
    const auto minimumFeasibleReplayLimit
        = std::max<uint64_t>(1, static_cast<uint64_t>(std::ceil(referenceSet.replayTightReference.replayTimeMs)));
    auto relaxedReplayLimit = static_cast<uint64_t>(std::floor(referenceSet.unconstrainedReference.replayTimeMs));
    if (relaxedReplayLimit < minimumFeasibleReplayLimit)
    {
        relaxedReplayLimit = std::max<uint64_t>(
            minimumFeasibleReplayLimit,
            static_cast<uint64_t>(std::ceil(referenceSet.unconstrainedReference.replayTimeMs)));
    }
    const auto replayBudgetRange = relaxedReplayLimit > minimumFeasibleReplayLimit ? relaxedReplayLimit - minimumFeasibleReplayLimit : 0U;
    scenario.replayLatencyLimitMs
        = minimumFeasibleReplayLimit
        + static_cast<uint64_t>(std::llround(latencyQuantile * static_cast<double>(replayBudgetRange)));
    scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;

    for (size_t hostIndex = 0; hostIndex < scenario.storageBudgetsBytes.size(); ++hostIndex)
    {
        const auto replayTightStorage = referenceSet.replayTightReference.storageBytesByHost.at(hostIndex);
        const auto maxStorage = std::max(replayTightStorage, referenceSet.maxStorageByHost.at(hostIndex));
        const auto storageBudget = replayTightStorage
            + static_cast<size_t>(std::llround(storageQuantile * static_cast<double>(maxStorage - replayTightStorage)));
        scenario.storageBudgetsBytes[hostIndex] = std::max<size_t>(1, std::min(maxStorage, storageBudget));
    }
}

void populateFrozenScenarioShape(const ScenarioData& scenario, FrozenScenario& frozenScenario)
{
    const auto optionSummary = summarizeCandidateOptions(scenario);
    frozenScenario.baselineMode = std::string(baselineModeName(scenario.baselineMode));
    frozenScenario.networkTopology = std::string(networkTopologyName(scenario.networkTopology));
    frozenScenario.hostCount = scenario.hosts.size();
    frozenScenario.nodeCount = scenario.nodes.size();
    frozenScenario.candidateEdges = optionSummary.candidateEdges;
    frozenScenario.candidateOptions = optionSummary.candidateOptions;
    frozenScenario.reuseOptions = optionSummary.reuseOptions;
    frozenScenario.upgradeOptions = optionSummary.upgradeOptions;
    frozenScenario.createOptions = optionSummary.createOptions;
}

[[nodiscard]] std::optional<FrozenScenario> chooseFrozenScenarioForBucket(
    const ScenarioData& baseScenario,
    const std::vector<CutMetrics>& allCuts,
    const CutMetrics& unconstrainedBest,
    const size_t treeHeight,
    const size_t fanout,
    const uint64_t seed,
    const size_t hostCount,
    const CorpusBucket& bucket,
    const std::vector<BudgetAttempt>& attempts)
{
    std::optional<FrozenScenario> bestScenario;
    auto bestScore = std::numeric_limits<double>::infinity();
    for (const auto& attempt : attempts)
    {
        auto scenario = baseScenario;
        applyBudgetQuantiles(allCuts, attempt.latencyQuantile, attempt.storageQuantile, scenario);

        ScenarioStats exactStats;
        try
        {
            exactStats = computeFeasibleCutStats(allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        }
        catch (const std::exception&)
        {
            continue;
        }

        if (exactStats.feasibleCutCount == 0 || exactStats.feasibleCutCount >= exactStats.validCutCount)
        {
            continue;
        }

        const auto currentFeasibleFraction = feasibleFraction(exactStats.feasibleCutCount, exactStats.validCutCount);
        if (currentFeasibleFraction < bucket.minFeasibleFraction || currentFeasibleFraction > bucket.maxFeasibleFraction)
        {
            continue;
        }
        if (isFeasibleCut(unconstrainedBest, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes))
        {
            continue;
        }
        if (!solverCanSolveScenario(scenario))
        {
            continue;
        }

        const auto score = std::abs(currentFeasibleFraction - bucketMidpoint(bucket));
        if (!bestScenario.has_value() || score < bestScore - EPSILON)
        {
            bestScore = score;
            bestScenario = FrozenScenario{
                .scenario = baseScenario.scenarioName,
                .instanceId = buildInstanceId(baseScenario, treeHeight, fanout, seed, bucket.name),
                .corpusBucket = bucket.name,
                .budgetProfile = attempt.profileName,
                .seed = seed,
                .treeHeight = treeHeight,
                .fanout = fanout,
                .hostCount = hostCount,
                .latencyQuantile = attempt.latencyQuantile,
                .storageQuantile = attempt.storageQuantile,
                .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
                .storageBudgetsBytes = scenario.storageBudgetsBytes,
                .validCutCount = exactStats.validCutCount,
                .feasibleCutCount = exactStats.feasibleCutCount,
                .feasibleFraction = currentFeasibleFraction,
                .bestMaintenanceCost = exactStats.bestMaintenanceCost,
                .bestReplayTimeMs = exactStats.bestReplayTimeMs,
                .averageMaintenanceCost = exactStats.averageMaintenanceCost,
                .worstMaintenanceCost = exactStats.worstMaintenanceCost,
                .unconstrainedBestMaintenanceCost = unconstrainedBest.maintenanceCost,
                .unconstrainedBestReplayTimeMs = unconstrainedBest.replayTimeMs,
                .constraintsBind = true};
            populateFrozenScenarioShape(scenario, *bestScenario);
        }
    }
    return bestScenario;
}

[[nodiscard]] std::optional<FrozenScenario> chooseFallbackFrozenScenario(
    const ScenarioData& baseScenario,
    const std::vector<CutMetrics>& allCuts,
    const CutMetrics& unconstrainedBest,
    const size_t treeHeight,
    const size_t fanout,
    const uint64_t seed,
    const size_t hostCount,
    const std::vector<BudgetAttempt>& attempts)
{
    std::optional<FrozenScenario> bestScenario;
    auto bestScore = std::numeric_limits<double>::infinity();
    for (const auto& attempt : attempts)
    {
        auto scenario = baseScenario;
        applyBudgetQuantiles(allCuts, attempt.latencyQuantile, attempt.storageQuantile, scenario);

        ScenarioStats exactStats;
        try
        {
            exactStats = computeFeasibleCutStats(allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        }
        catch (const std::exception&)
        {
            continue;
        }

        if (exactStats.feasibleCutCount == 0 || exactStats.feasibleCutCount >= exactStats.validCutCount)
        {
            continue;
        }
        if (!solverCanSolveScenario(scenario))
        {
            continue;
        }

        const auto currentFeasibleFraction = feasibleFraction(exactStats.feasibleCutCount, exactStats.validCutCount);
        const auto score = std::abs(currentFeasibleFraction - 0.35);
        const auto constraintsBind = !isFeasibleCut(unconstrainedBest, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        if (!bestScenario.has_value() || score < bestScore - EPSILON)
        {
            bestScore = score;
            bestScenario = FrozenScenario{
                .scenario = baseScenario.scenarioName,
                .instanceId = buildInstanceId(baseScenario, treeHeight, fanout, seed, "fallback"),
                .corpusBucket = "fallback",
                .budgetProfile = attempt.profileName,
                .seed = seed,
                .treeHeight = treeHeight,
                .fanout = fanout,
                .hostCount = hostCount,
                .latencyQuantile = attempt.latencyQuantile,
                .storageQuantile = attempt.storageQuantile,
                .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
                .storageBudgetsBytes = scenario.storageBudgetsBytes,
                .validCutCount = exactStats.validCutCount,
                .feasibleCutCount = exactStats.feasibleCutCount,
                .feasibleFraction = currentFeasibleFraction,
                .bestMaintenanceCost = exactStats.bestMaintenanceCost,
                .bestReplayTimeMs = exactStats.bestReplayTimeMs,
                .averageMaintenanceCost = exactStats.averageMaintenanceCost,
                .worstMaintenanceCost = exactStats.worstMaintenanceCost,
                .unconstrainedBestMaintenanceCost = unconstrainedBest.maintenanceCost,
                .unconstrainedBestReplayTimeMs = unconstrainedBest.replayTimeMs,
                .constraintsBind = constraintsBind};
            populateFrozenScenarioShape(scenario, *bestScenario);
        }
    }
    return bestScenario;
}

[[nodiscard]] std::optional<FrozenScenario> chooseDegenerateFrozenScenario(
    const ScenarioData& baseScenario,
    const std::vector<CutMetrics>& allCuts,
    const CutMetrics& unconstrainedBest,
    const size_t treeHeight,
    const size_t fanout,
    const uint64_t seed,
    const size_t hostCount,
    const std::vector<BudgetAttempt>& attempts)
{
    std::optional<FrozenScenario> bestScenario;
    auto bestScore = std::numeric_limits<double>::infinity();
    for (const auto& attempt : attempts)
    {
        auto scenario = baseScenario;
        applyBudgetQuantiles(allCuts, attempt.latencyQuantile, attempt.storageQuantile, scenario);

        ScenarioStats exactStats;
        try
        {
            exactStats = computeFeasibleCutStats(allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        }
        catch (const std::exception&)
        {
            continue;
        }
        if (!solverCanSolveScenario(scenario))
        {
            continue;
        }

        const auto currentFeasibleFraction = feasibleFraction(exactStats.feasibleCutCount, exactStats.validCutCount);
        const auto allCutsFeasiblePenalty = exactStats.feasibleCutCount == exactStats.validCutCount ? 1.0 : 0.0;
        const auto score = allCutsFeasiblePenalty + std::abs(currentFeasibleFraction - 0.50);
        const auto constraintsBind = !isFeasibleCut(unconstrainedBest, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        if (!bestScenario.has_value() || score < bestScore - EPSILON)
        {
            bestScore = score;
            bestScenario = FrozenScenario{
                .scenario = baseScenario.scenarioName,
                .instanceId = buildInstanceId(baseScenario, treeHeight, fanout, seed, "degenerate"),
                .corpusBucket = "degenerate",
                .budgetProfile = attempt.profileName,
                .seed = seed,
                .treeHeight = treeHeight,
                .fanout = fanout,
                .hostCount = hostCount,
                .latencyQuantile = attempt.latencyQuantile,
                .storageQuantile = attempt.storageQuantile,
                .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
                .storageBudgetsBytes = scenario.storageBudgetsBytes,
                .validCutCount = exactStats.validCutCount,
                .feasibleCutCount = exactStats.feasibleCutCount,
                .feasibleFraction = currentFeasibleFraction,
                .bestMaintenanceCost = exactStats.bestMaintenanceCost,
                .bestReplayTimeMs = exactStats.bestReplayTimeMs,
                .averageMaintenanceCost = exactStats.averageMaintenanceCost,
                .worstMaintenanceCost = exactStats.worstMaintenanceCost,
                .unconstrainedBestMaintenanceCost = unconstrainedBest.maintenanceCost,
                .unconstrainedBestReplayTimeMs = unconstrainedBest.replayTimeMs,
                .constraintsBind = constraintsBind};
            populateFrozenScenarioShape(scenario, *bestScenario);
        }
    }
    return bestScenario;
}

[[nodiscard]] std::vector<FrozenScenario> choosePerformanceOnlyFrozenScenarios(
    const BenchmarkConfig& config, const size_t treeHeight, const size_t fanout, const uint64_t seed)
{
    const auto hostCount = performanceScenarioHostCount(config, fanout, seed);
    const auto baseScenario = buildScenarioTemplate(config, treeHeight, fanout, hostCount, seed, BaselineMode::PerformanceOnly);
    const auto referenceSet = computePerformanceReferenceMetrics(baseScenario);
    const auto attempts = defaultPerformanceBudgetAttempts();

    std::vector<FrozenScenario> scenarios;
    scenarios.reserve(attempts.size());
    for (const auto& attempt : attempts)
    {
        auto scenario = baseScenario;
        applyHeuristicPerformanceBudgets(referenceSet, attempt.latencyQuantile, attempt.storageQuantile, scenario);
        if (!solverCanSolveScenario(scenario))
        {
            continue;
        }

        const auto constraintsBind =
            !isFeasibleCut(referenceSet.unconstrainedReference, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        auto frozenScenario = FrozenScenario{
            .scenario = baseScenario.scenarioName,
            .instanceId = buildInstanceId(baseScenario, treeHeight, fanout, seed, attempt.bucketName),
            .corpusBucket = attempt.bucketName,
            .budgetProfile = attempt.profileName,
            .seed = seed,
            .treeHeight = treeHeight,
            .fanout = fanout,
            .hostCount = hostCount,
            .latencyQuantile = attempt.latencyQuantile,
            .storageQuantile = attempt.storageQuantile,
            .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
            .storageBudgetsBytes = scenario.storageBudgetsBytes,
            .validCutCount = 0,
            .feasibleCutCount = 0,
            .feasibleFraction = 0.0,
            .bestMaintenanceCost = 0.0,
            .bestReplayTimeMs = 0.0,
            .averageMaintenanceCost = 0.0,
            .worstMaintenanceCost = 0.0,
            .unconstrainedBestMaintenanceCost = referenceSet.unconstrainedReference.maintenanceCost,
            .unconstrainedBestReplayTimeMs = referenceSet.unconstrainedReference.replayTimeMs,
            .constraintsBind = constraintsBind};
        populateFrozenScenarioShape(scenario, frozenScenario);
        scenarios.push_back(std::move(frozenScenario));
    }

    return scenarios;
}

[[nodiscard]] std::vector<FrozenScenario> generateFrozenCorpus(const BenchmarkConfig& config)
{
    std::vector<FrozenScenario> corpus;
    const auto buckets = defaultCorpusBuckets();
    const auto allAttempts = collectBudgetAttempts(buckets);
    for (size_t fanout = config.minFanout; fanout <= config.maxFanout; ++fanout)
    {
        for (uint64_t seed = config.firstSeed; seed < config.firstSeed + config.seedCount; ++seed)
        {
            if (config.corpusTier != CorpusTier::PerformanceOnly)
            {
                const auto scenarioHostCount = effectiveScenarioHostCount(config, fanout, seed);
                const auto baseScenario = buildScenarioTemplate(config, config.treeHeight, fanout, scenarioHostCount, seed);
                const auto allCuts = enumerateAllCuts(baseScenario);
                const auto unconstrainedBest = computeUnconstrainedBestCut(allCuts);
                bool addedAnyScenario = false;
                for (const auto& bucket : buckets)
                {
                    if (const auto frozenScenario = chooseFrozenScenarioForBucket(
                            baseScenario,
                            allCuts,
                            unconstrainedBest,
                            config.treeHeight,
                            fanout,
                            seed,
                            scenarioHostCount,
                            bucket,
                            allAttempts);
                        frozenScenario.has_value())
                    {
                        corpus.push_back(*frozenScenario);
                        addedAnyScenario = true;
                    }
                }
                if (!addedAnyScenario)
                {
                    if (const auto fallbackScenario = chooseFallbackFrozenScenario(
                            baseScenario, allCuts, unconstrainedBest, config.treeHeight, fanout, seed, scenarioHostCount, allAttempts);
                        fallbackScenario.has_value())
                    {
                        corpus.push_back(*fallbackScenario);
                        addedAnyScenario = true;
                    }
                }
                if (!addedAnyScenario)
                {
                    if (const auto degenerateScenario = chooseDegenerateFrozenScenario(
                            baseScenario, allCuts, unconstrainedBest, config.treeHeight, fanout, seed, scenarioHostCount, allAttempts);
                        degenerateScenario.has_value())
                    {
                        corpus.push_back(*degenerateScenario);
                    }
                }
            }

            if (config.scenarioFamily == ScenarioFamily::DagMix && config.corpusTier != CorpusTier::Exact)
            {
                for (auto performanceScenario : choosePerformanceOnlyFrozenScenarios(config, config.treeHeight, fanout, seed))
                {
                    corpus.push_back(std::move(performanceScenario));
                }
            }
        }
    }

    if (corpus.empty())
    {
        throw std::runtime_error("failed to generate any frozen corpus instances; try a lower tree height or wider fanout sweep");
    }
    return corpus;
}

[[nodiscard]] Json frozenScenarioToJson(const FrozenScenario& scenario)
{
    return Json{
        {"scenario", scenario.scenario},
        {"baseline_mode", scenario.baselineMode},
        {"network_topology", scenario.networkTopology},
        {"instance_id", scenario.instanceId},
        {"corpus_bucket", scenario.corpusBucket},
        {"budget_profile", scenario.budgetProfile},
        {"seed", scenario.seed},
        {"tree_height", scenario.treeHeight},
        {"fanout", scenario.fanout},
        {"host_count", scenario.hostCount},
        {"node_count", scenario.nodeCount},
        {"candidate_edges", scenario.candidateEdges},
        {"candidate_options", scenario.candidateOptions},
        {"reuse_options", scenario.reuseOptions},
        {"upgrade_options", scenario.upgradeOptions},
        {"create_options", scenario.createOptions},
        {"latency_quantile", scenario.latencyQuantile},
        {"storage_quantile", scenario.storageQuantile},
        {"replay_latency_limit_ms", scenario.replayLatencyLimitMs},
        {"storage_budgets_bytes", scenario.storageBudgetsBytes},
        {"valid_cut_count", scenario.validCutCount},
        {"feasible_cut_count", scenario.feasibleCutCount},
        {"feasible_fraction", scenario.feasibleFraction},
        {"best_feasible_maintenance_cost", scenario.bestMaintenanceCost},
        {"best_feasible_replay_time_ms", scenario.bestReplayTimeMs},
        {"avg_feasible_maintenance_cost", scenario.averageMaintenanceCost},
        {"worst_feasible_maintenance_cost", scenario.worstMaintenanceCost},
        {"unconstrained_best_maintenance_cost", scenario.unconstrainedBestMaintenanceCost},
        {"unconstrained_best_replay_time_ms", scenario.unconstrainedBestReplayTimeMs},
        {"constraints_bind", scenario.constraintsBind}};
}

[[nodiscard]] FrozenScenario frozenScenarioFromJson(const Json& jsonObject)
{
    return FrozenScenario{
        .scenario = jsonObject.at("scenario").get<std::string>(),
        .baselineMode = jsonObject.value("baseline_mode", std::string("exact")),
        .networkTopology = jsonObject.value("network_topology", std::string("line")),
        .instanceId = jsonObject.at("instance_id").get<std::string>(),
        .corpusBucket = jsonObject.at("corpus_bucket").get<std::string>(),
        .budgetProfile = jsonObject.at("budget_profile").get<std::string>(),
        .seed = jsonObject.at("seed").get<uint64_t>(),
        .treeHeight = jsonObject.at("tree_height").get<size_t>(),
        .fanout = jsonObject.at("fanout").get<size_t>(),
        .hostCount = jsonObject.at("host_count").get<size_t>(),
        .nodeCount = jsonObject.value("node_count", 0U),
        .candidateEdges = jsonObject.value("candidate_edges", 0U),
        .candidateOptions = jsonObject.value("candidate_options", 0U),
        .reuseOptions = jsonObject.value("reuse_options", 0U),
        .upgradeOptions = jsonObject.value("upgrade_options", 0U),
        .createOptions = jsonObject.value("create_options", 0U),
        .latencyQuantile = jsonObject.at("latency_quantile").get<double>(),
        .storageQuantile = jsonObject.at("storage_quantile").get<double>(),
        .replayLatencyLimitMs = jsonObject.at("replay_latency_limit_ms").get<uint64_t>(),
        .storageBudgetsBytes = jsonObject.at("storage_budgets_bytes").get<std::vector<size_t>>(),
        .validCutCount = jsonObject.at("valid_cut_count").get<size_t>(),
        .feasibleCutCount = jsonObject.at("feasible_cut_count").get<size_t>(),
        .feasibleFraction = jsonObject.at("feasible_fraction").get<double>(),
        .bestMaintenanceCost = jsonObject.at("best_feasible_maintenance_cost").get<double>(),
        .bestReplayTimeMs = jsonObject.at("best_feasible_replay_time_ms").get<double>(),
        .averageMaintenanceCost = jsonObject.at("avg_feasible_maintenance_cost").get<double>(),
        .worstMaintenanceCost = jsonObject.at("worst_feasible_maintenance_cost").get<double>(),
        .unconstrainedBestMaintenanceCost = jsonObject.at("unconstrained_best_maintenance_cost").get<double>(),
        .unconstrainedBestReplayTimeMs = jsonObject.at("unconstrained_best_replay_time_ms").get<double>(),
        .constraintsBind = jsonObject.at("constraints_bind").get<bool>()};
}

void writeFrozenCorpusFile(const std::vector<FrozenScenario>& corpus, const std::string& outputPath)
{
    Json root;
    root["schema_version"] = CORPUS_SCHEMA_VERSION;
    root["instances"] = Json::array();
    for (const auto& instance : corpus)
    {
        root["instances"].push_back(frozenScenarioToJson(instance));
    }

    std::ofstream outputFile(outputPath, std::ios::out | std::ios::trunc);
    if (!outputFile.is_open())
    {
        throw std::runtime_error("failed to open corpus output file: " + outputPath);
    }
    outputFile << root.dump(2) << '\n';
}

[[nodiscard]] std::vector<FrozenScenario> readFrozenCorpusFile(const std::string& inputPath)
{
    std::ifstream inputFile(inputPath);
    if (!inputFile.is_open())
    {
        throw std::runtime_error("failed to open corpus input file: " + inputPath);
    }

    Json root = Json::parse(inputFile);
    const auto schemaVersion = root.at("schema_version").get<size_t>();
    if (schemaVersion != 1 && schemaVersion != 2 && schemaVersion != CORPUS_SCHEMA_VERSION)
    {
        throw std::runtime_error(
            "unsupported corpus schema version " + std::to_string(schemaVersion) + ", expected 1, 2, or "
            + std::to_string(CORPUS_SCHEMA_VERSION));
    }

    std::vector<FrozenScenario> corpus;
    for (const auto& instance : root.at("instances"))
    {
        corpus.push_back(frozenScenarioFromJson(instance));
    }
    if (corpus.empty())
    {
        throw std::runtime_error("corpus file does not contain any instances");
    }
    return corpus;
}

void verifyFrozenScenarioShape(const FrozenScenario& frozenScenario, const ScenarioData& scenario)
{
    const auto optionSummary = summarizeCandidateOptions(scenario);
    if (frozenScenario.baselineMode != baselineModeName(scenario.baselineMode))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": baseline mode changed");
    }
    if (frozenScenario.networkTopology != networkTopologyName(scenario.networkTopology))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": network topology changed");
    }
    if (frozenScenario.hostCount != scenario.hosts.size())
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": host count changed");
    }
    if ((frozenScenario.nodeCount != 0 && frozenScenario.nodeCount != scenario.nodes.size())
        || (frozenScenario.candidateEdges != 0 && frozenScenario.candidateEdges != optionSummary.candidateEdges)
        || (frozenScenario.candidateOptions != 0 && frozenScenario.candidateOptions != optionSummary.candidateOptions)
        || (frozenScenario.reuseOptions != 0 && frozenScenario.reuseOptions != optionSummary.reuseOptions)
        || (frozenScenario.upgradeOptions != 0 && frozenScenario.upgradeOptions != optionSummary.upgradeOptions)
        || (frozenScenario.createOptions != 0 && frozenScenario.createOptions != optionSummary.createOptions))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": scenario shape changed");
    }
    if (frozenScenario.storageBudgetsBytes != scenario.storageBudgetsBytes || frozenScenario.replayLatencyLimitMs != scenario.replayLatencyLimitMs)
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": scenario budgets changed");
    }
}

void verifyExactFrozenScenario(
    const FrozenScenario& frozenScenario, const ScenarioStats& exactStats, const CutMetrics& unconstrainedBest, const ScenarioData& scenario)
{
    verifyFrozenScenarioShape(frozenScenario, scenario);
    if (frozenScenario.validCutCount != exactStats.validCutCount || frozenScenario.feasibleCutCount != exactStats.feasibleCutCount)
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": cut counts changed");
    }
    if (!almostEqual(frozenScenario.bestMaintenanceCost, exactStats.bestMaintenanceCost)
        || !almostEqual(frozenScenario.bestReplayTimeMs, exactStats.bestReplayTimeMs)
        || !almostEqual(frozenScenario.averageMaintenanceCost, exactStats.averageMaintenanceCost)
        || !almostEqual(frozenScenario.worstMaintenanceCost, exactStats.worstMaintenanceCost))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": exact feasible baseline changed");
    }
    if (!almostEqual(frozenScenario.unconstrainedBestMaintenanceCost, unconstrainedBest.maintenanceCost)
        || !almostEqual(frozenScenario.unconstrainedBestReplayTimeMs, unconstrainedBest.replayTimeMs))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": unconstrained baseline changed");
    }
}

void verifyPerformanceFrozenScenario(const FrozenScenario& frozenScenario, const CutMetrics& referenceMetrics, const ScenarioData& scenario)
{
    verifyFrozenScenarioShape(frozenScenario, scenario);
    if (!almostEqual(frozenScenario.unconstrainedBestMaintenanceCost, referenceMetrics.maintenanceCost)
        || !almostEqual(frozenScenario.unconstrainedBestReplayTimeMs, referenceMetrics.replayTimeMs))
    {
        throw std::runtime_error("frozen corpus drift detected for " + frozenScenario.instanceId + ": performance reference changed");
    }
}

[[nodiscard]] ShadowPriceTuning makeShadowPriceTuning(
    const double replayInitialPriceScale = 1.0,
    const double replayStepScale = 1.0,
    const double storageStepScale = 1.0,
    const size_t maxIterations = DEFAULT_SHADOW_PRICE_ITERATIONS)
{
    return ShadowPriceTuning{
        .replayInitialPriceScale = replayInitialPriceScale,
        .replayStepScale = replayStepScale,
        .storageStepScale = storageStepScale,
        .maxIterations = maxIterations};
}

[[nodiscard]] ScenarioRow buildScenarioRow(
    const ScenarioData& scenario,
    const std::vector<CutMetrics>& allCuts,
    const ScenarioStats& exactStats,
    const CutMetrics& unconstrainedBest,
    const RecordingBoundarySelection& selection,
    const double solverTimeNsMean,
    const std::string& benchmarkMode,
    const std::string& instanceId,
    const std::string& corpusBucket,
    const std::string& budgetProfile,
    const double currentFeasibleFraction,
    const double latencyQuantile,
    const double storageQuantile,
    const bool constraintsBind,
    const ShadowPriceTuning& shadowPriceTuning)
{
    const auto optionSummary = summarizeCandidateOptions(scenario);
    size_t visitedSelectedEdges = 0;
    const auto selectedMetrics = evaluateSelectionMetrics(scenario, selection, visitedSelectedEdges);
    if (visitedSelectedEdges != selection.selectedBoundary.size())
    {
        throw std::runtime_error("solver returned a non-boundary cut for the synthetic scenario");
    }
    if (!isFeasibleCut(selectedMetrics, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes))
    {
        throw std::runtime_error("solver returned an infeasible selection for the synthetic scenario");
    }

    const auto selectedRank = computeSelectionRank(selectedMetrics, allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
    return ScenarioRow{
        .scenario = scenario.scenarioName,
        .baselineMode = std::string(baselineModeName(scenario.baselineMode)),
        .networkTopology = std::string(networkTopologyName(scenario.networkTopology)),
        .seed = 0,
        .treeHeight = 0,
        .fanout = 0,
        .hostCount = scenario.hosts.size(),
        .nodeCount = scenario.nodes.size(),
        .candidateEdges = optionSummary.candidateEdges,
        .candidateOptions = optionSummary.candidateOptions,
        .reuseOptions = optionSummary.reuseOptions,
        .upgradeOptions = optionSummary.upgradeOptions,
        .createOptions = optionSummary.createOptions,
        .leafCount = scenario.candidateSet.leafOperatorIds.size(),
        .validCutCount = exactStats.validCutCount,
        .feasibleCutCount = exactStats.feasibleCutCount,
        .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
        .selectedEdgeCount = selectedMetrics.selectedEdgeCount,
        .selectedMaintenanceCost = selectedMetrics.maintenanceCost,
        .selectedReplayTimeMs = selectedMetrics.replayTimeMs,
        .bestMaintenanceCost = exactStats.bestMaintenanceCost,
        .bestReplayTimeMs = exactStats.bestReplayTimeMs,
        .averageMaintenanceCost = exactStats.averageMaintenanceCost,
        .worstMaintenanceCost = exactStats.worstMaintenanceCost,
        .selectedOverBest = positiveRatio(selectedMetrics.maintenanceCost, exactStats.bestMaintenanceCost),
        .averageOverBest = positiveRatio(exactStats.averageMaintenanceCost, exactStats.bestMaintenanceCost),
        .worstOverBest = positiveRatio(exactStats.worstMaintenanceCost, exactStats.bestMaintenanceCost),
        .selectedRank = selectedRank,
        .solverTimeNsMean = solverTimeNsMean,
        .benchmarkMode = benchmarkMode,
        .instanceId = instanceId,
        .corpusBucket = corpusBucket,
        .budgetProfile = budgetProfile,
        .feasibleFraction = currentFeasibleFraction,
        .latencyQuantile = latencyQuantile,
        .storageQuantile = storageQuantile,
        .unconstrainedBestMaintenanceCost = unconstrainedBest.maintenanceCost,
        .unconstrainedBestReplayTimeMs = unconstrainedBest.replayTimeMs,
        .constraintsBind = constraintsBind,
        .replayInitialPriceScale = shadowPriceTuning.replayInitialPriceScale,
        .replayStepScale = shadowPriceTuning.replayStepScale,
        .storageStepScale = shadowPriceTuning.storageStepScale,
        .shadowPriceIterations = shadowPriceTuning.maxIterations,
        .solverSucceeded = true,
        .optimalHit = selectedRank == 1};
}

[[nodiscard]] ScenarioRow buildFailedScenarioRow(
    const ScenarioData& scenario,
    const ScenarioStats& exactStats,
    const CutMetrics& unconstrainedBest,
    const double solverTimeNsMean,
    const std::string& benchmarkMode,
    const std::string& instanceId,
    const std::string& corpusBucket,
    const std::string& budgetProfile,
    const double currentFeasibleFraction,
    const double latencyQuantile,
    const double storageQuantile,
    const bool constraintsBind,
    const ShadowPriceTuning& shadowPriceTuning)
{
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    const auto optionSummary = summarizeCandidateOptions(scenario);
    return ScenarioRow{
        .scenario = scenario.scenarioName,
        .baselineMode = std::string(baselineModeName(scenario.baselineMode)),
        .networkTopology = std::string(networkTopologyName(scenario.networkTopology)),
        .seed = 0,
        .treeHeight = 0,
        .fanout = 0,
        .hostCount = scenario.hosts.size(),
        .nodeCount = scenario.nodes.size(),
        .candidateEdges = optionSummary.candidateEdges,
        .candidateOptions = optionSummary.candidateOptions,
        .reuseOptions = optionSummary.reuseOptions,
        .upgradeOptions = optionSummary.upgradeOptions,
        .createOptions = optionSummary.createOptions,
        .leafCount = scenario.candidateSet.leafOperatorIds.size(),
        .validCutCount = exactStats.validCutCount,
        .feasibleCutCount = exactStats.feasibleCutCount,
        .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
        .selectedEdgeCount = 0,
        .selectedMaintenanceCost = nan,
        .selectedReplayTimeMs = nan,
        .bestMaintenanceCost = exactStats.bestMaintenanceCost,
        .bestReplayTimeMs = exactStats.bestReplayTimeMs,
        .averageMaintenanceCost = exactStats.averageMaintenanceCost,
        .worstMaintenanceCost = exactStats.worstMaintenanceCost,
        .selectedOverBest = nan,
        .averageOverBest = positiveRatio(exactStats.averageMaintenanceCost, exactStats.bestMaintenanceCost),
        .worstOverBest = positiveRatio(exactStats.worstMaintenanceCost, exactStats.bestMaintenanceCost),
        .selectedRank = exactStats.feasibleCutCount + 1,
        .solverTimeNsMean = solverTimeNsMean,
        .benchmarkMode = benchmarkMode,
        .instanceId = instanceId,
        .corpusBucket = corpusBucket,
        .budgetProfile = budgetProfile,
        .feasibleFraction = currentFeasibleFraction,
        .latencyQuantile = latencyQuantile,
        .storageQuantile = storageQuantile,
        .unconstrainedBestMaintenanceCost = unconstrainedBest.maintenanceCost,
        .unconstrainedBestReplayTimeMs = unconstrainedBest.replayTimeMs,
        .constraintsBind = constraintsBind,
        .replayInitialPriceScale = shadowPriceTuning.replayInitialPriceScale,
        .replayStepScale = shadowPriceTuning.replayStepScale,
        .storageStepScale = shadowPriceTuning.storageStepScale,
        .shadowPriceIterations = shadowPriceTuning.maxIterations,
        .solverSucceeded = false,
        .optimalHit = false};
}

[[nodiscard]] ScenarioRow buildPerformanceOnlyScenarioRow(
    const ScenarioData& scenario,
    const CutMetrics& unconstrainedReference,
    const RecordingBoundarySelection& selection,
    const double solverTimeNsMean,
    const std::string& benchmarkMode,
    const std::string& instanceId,
    const std::string& corpusBucket,
    const std::string& budgetProfile,
    const double latencyQuantile,
    const double storageQuantile,
    const bool constraintsBind,
    const ShadowPriceTuning& shadowPriceTuning)
{
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    const auto optionSummary = summarizeCandidateOptions(scenario);
    size_t visitedSelectedEdges = 0;
    const auto selectedMetrics = evaluateSelectionMetrics(scenario, selection, visitedSelectedEdges);
    if (visitedSelectedEdges != selection.selectedBoundary.size())
    {
        throw std::runtime_error("solver returned a non-boundary cut for the synthetic scenario");
    }
    if (!isFeasibleCut(selectedMetrics, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes))
    {
        throw std::runtime_error("solver returned an infeasible selection for the synthetic scenario");
    }

    return ScenarioRow{
        .scenario = scenario.scenarioName,
        .baselineMode = std::string(baselineModeName(scenario.baselineMode)),
        .networkTopology = std::string(networkTopologyName(scenario.networkTopology)),
        .seed = 0,
        .treeHeight = 0,
        .fanout = 0,
        .hostCount = scenario.hosts.size(),
        .nodeCount = scenario.nodes.size(),
        .candidateEdges = optionSummary.candidateEdges,
        .candidateOptions = optionSummary.candidateOptions,
        .reuseOptions = optionSummary.reuseOptions,
        .upgradeOptions = optionSummary.upgradeOptions,
        .createOptions = optionSummary.createOptions,
        .leafCount = scenario.candidateSet.leafOperatorIds.size(),
        .validCutCount = 0,
        .feasibleCutCount = 0,
        .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
        .selectedEdgeCount = selectedMetrics.selectedEdgeCount,
        .selectedMaintenanceCost = selectedMetrics.maintenanceCost,
        .selectedReplayTimeMs = selectedMetrics.replayTimeMs,
        .bestMaintenanceCost = nan,
        .bestReplayTimeMs = nan,
        .averageMaintenanceCost = nan,
        .worstMaintenanceCost = nan,
        .selectedOverBest = nan,
        .averageOverBest = nan,
        .worstOverBest = nan,
        .selectedRank = 0,
        .solverTimeNsMean = solverTimeNsMean,
        .benchmarkMode = benchmarkMode,
        .instanceId = instanceId,
        .corpusBucket = corpusBucket,
        .budgetProfile = budgetProfile,
        .feasibleFraction = 0.0,
        .latencyQuantile = latencyQuantile,
        .storageQuantile = storageQuantile,
        .unconstrainedBestMaintenanceCost = unconstrainedReference.maintenanceCost,
        .unconstrainedBestReplayTimeMs = unconstrainedReference.replayTimeMs,
        .constraintsBind = constraintsBind,
        .replayInitialPriceScale = shadowPriceTuning.replayInitialPriceScale,
        .replayStepScale = shadowPriceTuning.replayStepScale,
        .storageStepScale = shadowPriceTuning.storageStepScale,
        .shadowPriceIterations = shadowPriceTuning.maxIterations,
        .solverSucceeded = true,
        .optimalHit = false};
}

[[nodiscard]] ScenarioRow buildFailedPerformanceOnlyScenarioRow(
    const ScenarioData& scenario,
    const CutMetrics& unconstrainedReference,
    const double solverTimeNsMean,
    const std::string& benchmarkMode,
    const std::string& instanceId,
    const std::string& corpusBucket,
    const std::string& budgetProfile,
    const double latencyQuantile,
    const double storageQuantile,
    const bool constraintsBind,
    const ShadowPriceTuning& shadowPriceTuning)
{
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    const auto optionSummary = summarizeCandidateOptions(scenario);
    return ScenarioRow{
        .scenario = scenario.scenarioName,
        .baselineMode = std::string(baselineModeName(scenario.baselineMode)),
        .networkTopology = std::string(networkTopologyName(scenario.networkTopology)),
        .seed = 0,
        .treeHeight = 0,
        .fanout = 0,
        .hostCount = scenario.hosts.size(),
        .nodeCount = scenario.nodes.size(),
        .candidateEdges = optionSummary.candidateEdges,
        .candidateOptions = optionSummary.candidateOptions,
        .reuseOptions = optionSummary.reuseOptions,
        .upgradeOptions = optionSummary.upgradeOptions,
        .createOptions = optionSummary.createOptions,
        .leafCount = scenario.candidateSet.leafOperatorIds.size(),
        .validCutCount = 0,
        .feasibleCutCount = 0,
        .replayLatencyLimitMs = scenario.replayLatencyLimitMs,
        .selectedEdgeCount = 0,
        .selectedMaintenanceCost = nan,
        .selectedReplayTimeMs = nan,
        .bestMaintenanceCost = nan,
        .bestReplayTimeMs = nan,
        .averageMaintenanceCost = nan,
        .worstMaintenanceCost = nan,
        .selectedOverBest = nan,
        .averageOverBest = nan,
        .worstOverBest = nan,
        .selectedRank = 0,
        .solverTimeNsMean = solverTimeNsMean,
        .benchmarkMode = benchmarkMode,
        .instanceId = instanceId,
        .corpusBucket = corpusBucket,
        .budgetProfile = budgetProfile,
        .feasibleFraction = 0.0,
        .latencyQuantile = latencyQuantile,
        .storageQuantile = storageQuantile,
        .unconstrainedBestMaintenanceCost = unconstrainedReference.maintenanceCost,
        .unconstrainedBestReplayTimeMs = unconstrainedReference.replayTimeMs,
        .constraintsBind = constraintsBind,
        .replayInitialPriceScale = shadowPriceTuning.replayInitialPriceScale,
        .replayStepScale = shadowPriceTuning.replayStepScale,
        .storageStepScale = shadowPriceTuning.storageStepScale,
        .shadowPriceIterations = shadowPriceTuning.maxIterations,
        .solverSucceeded = false,
        .optimalHit = false};
}

[[nodiscard]] ScenarioRow runAdaptiveScenario(const BenchmarkConfig& config, const size_t fanout, const uint64_t seed)
{
    auto scenario = buildScenarioTemplate(
        config,
        config.treeHeight,
        fanout,
        effectiveScenarioHostCount(config, fanout, seed),
        seed);
    const auto allCuts = enumerateAllCuts(scenario);
    const auto unconstrainedBest = computeUnconstrainedBestCut(allCuts);
    const auto defaultShadowPriceTuning = makeShadowPriceTuning();

    auto latencyQuantile = config.latencyQuantile;
    auto storageQuantile = config.storageQuantile;
    std::optional<RecordingBoundarySelection> selection;
    SharedPtr<WorkerCatalog> workerCatalog;
    std::shared_ptr<const WorkerCatalog> constWorkerCatalog;

    while (!selection.has_value())
    {
        deriveScenarioBudgets(allCuts, latencyQuantile, storageQuantile, allCuts.size(), scenario, latencyQuantile, storageQuantile);

        workerCatalog = makeWorkerCatalog(scenario);
        constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
        try
        {
            selection = RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{constWorkerCatalog}, defaultShadowPriceTuning).solve(scenario.candidateSet);
        }
        catch (const std::exception&)
        {
            if (latencyQuantile >= 1.0 && storageQuantile >= 1.0)
            {
                throw;
            }
            latencyQuantile = std::min(1.0, latencyQuantile + 0.05);
            storageQuantile = std::min(1.0, storageQuantile + 0.05);
        }
    }

    const auto exactStats = computeFeasibleCutStats(allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
    const auto solverTimeNsMean =
        measureSolverTimeNsMean(scenario.candidateSet, constWorkerCatalog, config.solverRepetitions, defaultShadowPriceTuning);
    auto row = buildScenarioRow(
        scenario,
        allCuts,
        exactStats,
        unconstrainedBest,
        *selection,
        solverTimeNsMean,
        "adaptive",
        buildInstanceId(scenario, config.treeHeight, fanout, seed, "adaptive"),
        "all-feasible",
        "adaptive-relaxation",
        feasibleFraction(exactStats.feasibleCutCount, exactStats.validCutCount),
        latencyQuantile,
        storageQuantile,
        false,
        defaultShadowPriceTuning);
    row.seed = seed;
    row.treeHeight = config.treeHeight;
    row.fanout = fanout;
    return row;
}

[[nodiscard]] std::string baseScenarioKey(const FrozenScenario& frozenScenario)
{
    return frozenScenario.scenario + "-" + frozenScenario.baselineMode + "-" + frozenScenario.networkTopology + "-h"
        + std::to_string(frozenScenario.treeHeight) + "-f"
        + std::to_string(frozenScenario.fanout) + "-hosts" + std::to_string(frozenScenario.hostCount) + "-s"
        + std::to_string(frozenScenario.seed);
}

void ensureBaseScenarioCacheEntry(
    const FrozenScenario& frozenScenario, std::unordered_map<std::string, BaseScenarioCacheEntry>& baseScenarioCache)
{
    const auto cacheKey = baseScenarioKey(frozenScenario);
    if (baseScenarioCache.contains(cacheKey))
    {
        return;
    }

    auto baseScenario = buildScenarioTemplate(frozenScenario);
    if (hasExactBaseline(parseBaselineMode(frozenScenario.baselineMode)))
    {
        auto allCuts = enumerateAllCuts(baseScenario);
        auto unconstrainedBest = computeUnconstrainedBestCut(allCuts);
        baseScenarioCache.emplace(
            cacheKey,
            BaseScenarioCacheEntry{
                .baseScenario = std::move(baseScenario),
                .allCuts = std::move(allCuts),
                .unconstrainedBest = unconstrainedBest});
        return;
    }

    const auto referenceSet = computePerformanceReferenceMetrics(baseScenario);
    baseScenarioCache.emplace(
        cacheKey,
        BaseScenarioCacheEntry{
            .baseScenario = std::move(baseScenario),
            .allCuts = {},
            .unconstrainedBest = referenceSet.unconstrainedReference});
}

[[nodiscard]] ScenarioRow runFrozenScenario(
    const BenchmarkConfig& config,
    const FrozenScenario& frozenScenario,
    std::unordered_map<std::string, BaseScenarioCacheEntry>& baseScenarioCache)
{
    const auto cacheKey = baseScenarioKey(frozenScenario);
    ensureBaseScenarioCacheEntry(frozenScenario, baseScenarioCache);

    const auto& cacheEntry = baseScenarioCache.at(cacheKey);
    auto scenario = cacheEntry.baseScenario;
    if (scenario.hosts.size() != frozenScenario.storageBudgetsBytes.size())
    {
        throw std::runtime_error("frozen corpus instance has mismatched storage budget vector: " + frozenScenario.instanceId);
    }
    scenario.storageBudgetsBytes = frozenScenario.storageBudgetsBytes;
    scenario.replayLatencyLimitMs = frozenScenario.replayLatencyLimitMs;
    scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;

    const auto workerCatalog = makeWorkerCatalog(scenario);
    const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
    const auto defaultShadowPriceTuning = makeShadowPriceTuning();
    ScenarioRow row;
    if (hasExactBaseline(parseBaselineMode(frozenScenario.baselineMode)))
    {
        const auto exactStats = computeFeasibleCutStats(cacheEntry.allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        verifyExactFrozenScenario(frozenScenario, exactStats, cacheEntry.unconstrainedBest, scenario);

        const auto selection =
            RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{constWorkerCatalog}, defaultShadowPriceTuning).solve(scenario.candidateSet);
        const auto solverTimeNsMean =
            measureSolverTimeNsMean(scenario.candidateSet, constWorkerCatalog, config.solverRepetitions, defaultShadowPriceTuning);
        row = buildScenarioRow(
            scenario,
            cacheEntry.allCuts,
            exactStats,
            cacheEntry.unconstrainedBest,
            selection,
            solverTimeNsMean,
            "frozen",
            frozenScenario.instanceId,
            frozenScenario.corpusBucket,
            frozenScenario.budgetProfile,
            frozenScenario.feasibleFraction,
            frozenScenario.latencyQuantile,
            frozenScenario.storageQuantile,
            frozenScenario.constraintsBind,
            defaultShadowPriceTuning);
    }
    else
    {
        verifyPerformanceFrozenScenario(frozenScenario, cacheEntry.unconstrainedBest, scenario);

        const auto selection =
            RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{constWorkerCatalog}, defaultShadowPriceTuning).solve(scenario.candidateSet);
        const auto solverTimeNsMean =
            measureSolverTimeNsMean(scenario.candidateSet, constWorkerCatalog, config.solverRepetitions, defaultShadowPriceTuning);
        row = buildPerformanceOnlyScenarioRow(
            scenario,
            cacheEntry.unconstrainedBest,
            selection,
            solverTimeNsMean,
            "frozen",
            frozenScenario.instanceId,
            frozenScenario.corpusBucket,
            frozenScenario.budgetProfile,
            frozenScenario.latencyQuantile,
            frozenScenario.storageQuantile,
            frozenScenario.constraintsBind,
            defaultShadowPriceTuning);
    }
    row.seed = frozenScenario.seed;
    row.treeHeight = frozenScenario.treeHeight;
    row.fanout = frozenScenario.fanout;
    return row;
}

[[nodiscard]] ScenarioRow runFrozenScenarioShadowPriceSweep(
    const BenchmarkConfig& config,
    const FrozenScenario& frozenScenario,
    const double replayInitialPriceScale,
    const double replayStepScale,
    std::unordered_map<std::string, BaseScenarioCacheEntry>& baseScenarioCache)
{
    const auto cacheKey = baseScenarioKey(frozenScenario);
    ensureBaseScenarioCacheEntry(frozenScenario, baseScenarioCache);

    const auto& cacheEntry = baseScenarioCache.at(cacheKey);
    auto scenario = cacheEntry.baseScenario;
    if (scenario.hosts.size() != frozenScenario.storageBudgetsBytes.size())
    {
        throw std::runtime_error("frozen corpus instance has mismatched storage budget vector: " + frozenScenario.instanceId);
    }
    scenario.storageBudgetsBytes = frozenScenario.storageBudgetsBytes;
    scenario.replayLatencyLimitMs = frozenScenario.replayLatencyLimitMs;
    scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;

    const auto workerCatalog = makeWorkerCatalog(scenario);
    const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
    const auto shadowPriceTuning =
        makeShadowPriceTuning(
            replayInitialPriceScale,
            replayStepScale,
            config.storageStepScale,
            config.shadowPriceIterations);
    const auto solverResult = runSolverOnce(scenario.candidateSet, constWorkerCatalog, shadowPriceTuning);
    ScenarioRow row;
    if (hasExactBaseline(parseBaselineMode(frozenScenario.baselineMode)))
    {
        const auto exactStats = computeFeasibleCutStats(cacheEntry.allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        verifyExactFrozenScenario(frozenScenario, exactStats, cacheEntry.unconstrainedBest, scenario);
        row = solverResult.succeeded
            ? buildScenarioRow(
                  scenario,
                  cacheEntry.allCuts,
                  exactStats,
                  cacheEntry.unconstrainedBest,
                  *solverResult.selection,
                  solverResult.elapsedNs,
                  "shadow-price-sweep",
                  frozenScenario.instanceId,
                  frozenScenario.corpusBucket,
                  frozenScenario.budgetProfile,
                  frozenScenario.feasibleFraction,
                  frozenScenario.latencyQuantile,
                  frozenScenario.storageQuantile,
                  frozenScenario.constraintsBind,
                  shadowPriceTuning)
            : buildFailedScenarioRow(
                  scenario,
                  exactStats,
                  cacheEntry.unconstrainedBest,
                  solverResult.elapsedNs,
                  "shadow-price-sweep",
                  frozenScenario.instanceId,
                  frozenScenario.corpusBucket,
                  frozenScenario.budgetProfile,
                  frozenScenario.feasibleFraction,
                  frozenScenario.latencyQuantile,
                  frozenScenario.storageQuantile,
                  frozenScenario.constraintsBind,
                  shadowPriceTuning);
    }
    else
    {
        verifyPerformanceFrozenScenario(frozenScenario, cacheEntry.unconstrainedBest, scenario);
        row = solverResult.succeeded
            ? buildPerformanceOnlyScenarioRow(
                  scenario,
                  cacheEntry.unconstrainedBest,
                  *solverResult.selection,
                  solverResult.elapsedNs,
                  "shadow-price-sweep",
                  frozenScenario.instanceId,
                  frozenScenario.corpusBucket,
                  frozenScenario.budgetProfile,
                  frozenScenario.latencyQuantile,
                  frozenScenario.storageQuantile,
                  frozenScenario.constraintsBind,
                  shadowPriceTuning)
            : buildFailedPerformanceOnlyScenarioRow(
                  scenario,
                  cacheEntry.unconstrainedBest,
                  solverResult.elapsedNs,
                  "shadow-price-sweep",
                  frozenScenario.instanceId,
                  frozenScenario.corpusBucket,
                  frozenScenario.budgetProfile,
                  frozenScenario.latencyQuantile,
                  frozenScenario.storageQuantile,
                  frozenScenario.constraintsBind,
                  shadowPriceTuning);
    }
    row.seed = frozenScenario.seed;
    row.treeHeight = frozenScenario.treeHeight;
    row.fanout = frozenScenario.fanout;
    return row;
}

[[nodiscard]] std::vector<ConvergenceTraceRow> runFrozenScenarioShadowPriceConvergence(
    const BenchmarkConfig& config,
    const FrozenScenario& frozenScenario,
    std::unordered_map<std::string, BaseScenarioCacheEntry>& baseScenarioCache)
{
    const auto cacheKey = baseScenarioKey(frozenScenario);
    ensureBaseScenarioCacheEntry(frozenScenario, baseScenarioCache);

    const auto& cacheEntry = baseScenarioCache.at(cacheKey);
    auto scenario = cacheEntry.baseScenario;
    if (scenario.hosts.size() != frozenScenario.storageBudgetsBytes.size())
    {
        throw std::runtime_error("frozen corpus instance has mismatched storage budget vector: " + frozenScenario.instanceId);
    }
    scenario.storageBudgetsBytes = frozenScenario.storageBudgetsBytes;
    scenario.replayLatencyLimitMs = frozenScenario.replayLatencyLimitMs;
    scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;
    if (hasExactBaseline(parseBaselineMode(frozenScenario.baselineMode)))
    {
        const auto exactStats = computeFeasibleCutStats(cacheEntry.allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
        verifyExactFrozenScenario(frozenScenario, exactStats, cacheEntry.unconstrainedBest, scenario);
    }
    else
    {
        verifyPerformanceFrozenScenario(frozenScenario, cacheEntry.unconstrainedBest, scenario);
    }

    const auto workerCatalog = makeWorkerCatalog(scenario);
    const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
    const auto shadowPriceTuning = makeShadowPriceTuning(
        config.replayInitialPriceScales.front(),
        config.replayStepScales.front(),
        config.storageStepScale,
        config.shadowPriceIterations);

    std::vector<ShadowPriceIterationStats> iterationTrace;
    bool solverSucceeded = false;
    try
    {
        static_cast<void>(RecordingBoundarySolver(SharedPtr<const WorkerCatalog>{constWorkerCatalog}, shadowPriceTuning)
                              .solve(scenario.candidateSet, &iterationTrace));
        solverSucceeded = true;
    }
    catch (const std::exception&)
    {
        solverSucceeded = false;
    }

    if (iterationTrace.empty())
    {
        throw std::runtime_error("shadow-price convergence trace did not record any iterations for " + frozenScenario.instanceId);
    }

    std::vector<ConvergenceTraceRow> rows;
    rows.reserve(shadowPriceTuning.maxIterations);
    auto lastIterationStats = iterationTrace.back();
    bool feasibleIncumbentAvailable = false;
    for (size_t iteration = 1; iteration <= shadowPriceTuning.maxIterations; ++iteration)
    {
        const auto& iterationStats = iteration <= iterationTrace.size() ? iterationTrace.at(iteration - 1) : lastIterationStats;
        const auto currentIterateFeasible =
            iterationStats.replayConstraintSatisfied && iterationStats.storageBudgetConstraintSatisfied;
        feasibleIncumbentAvailable = feasibleIncumbentAvailable || currentIterateFeasible;
        rows.push_back(ConvergenceTraceRow{
            .scenario = scenario.scenarioName,
            .baselineMode = std::string(baselineModeName(scenario.baselineMode)),
            .networkTopology = std::string(networkTopologyName(scenario.networkTopology)),
            .instanceId = frozenScenario.instanceId,
            .corpusBucket = frozenScenario.corpusBucket,
            .budgetProfile = frozenScenario.budgetProfile,
            .seed = frozenScenario.seed,
            .treeHeight = frozenScenario.treeHeight,
            .fanout = frozenScenario.fanout,
            .hostCount = scenario.hosts.size(),
            .nodeCount = scenario.nodes.size(),
            .candidateEdges = scenario.candidateSet.candidates.size(),
            .candidateOptions = summarizeCandidateOptions(scenario).candidateOptions,
            .iteration = iteration,
            .constraintsBind = frozenScenario.constraintsBind,
            .feasibleFraction = frozenScenario.feasibleFraction,
            .replayInitialPriceScale = shadowPriceTuning.replayInitialPriceScale,
            .replayStepScale = shadowPriceTuning.replayStepScale,
            .storageStepScale = shadowPriceTuning.storageStepScale,
            .shadowPriceIterations = shadowPriceTuning.maxIterations,
            .solverSucceeded = solverSucceeded,
            .replayTimePrice = iterationStats.replayTimePrice,
            .selectedReplayTimeMs = iterationStats.selectedReplayTimeMs,
            .replayConstraintSatisfactionPercent = iterationStats.replayConstraintSatisfactionPercent,
            .storageBudgetConstraintSatisfactionPercent = iterationStats.storageBudgetConstraintSatisfactionPercent,
            .maxStorageUtilizationPercent = iterationStats.maxStorageUtilizationPercent,
            .replayConstraintSatisfied = iterationStats.replayConstraintSatisfied,
            .storageBudgetConstraintSatisfied = iterationStats.storageBudgetConstraintSatisfied,
            .currentIterateFeasible = currentIterateFeasible,
            .returnableSolutionFeasible = feasibleIncumbentAvailable});
    }
    return rows;
}

[[nodiscard]] std::unordered_map<RecordingPlanEdge, size_t, RecordingPlanEdgeHash> buildEdgeIndexMap(const ScenarioData& scenario)
{
    std::unordered_map<RecordingPlanEdge, size_t, RecordingPlanEdgeHash> edgeIndexByPlanEdge;
    edgeIndexByPlanEdge.reserve(scenario.edges.size());
    for (size_t edgeIndex = 0; edgeIndex < scenario.edges.size(); ++edgeIndex)
    {
        edgeIndexByPlanEdge.emplace(scenario.edges[edgeIndex].edge, edgeIndex);
    }
    return edgeIndexByPlanEdge;
}

[[nodiscard]] Json serializeBoundarySelection(
    const ScenarioData& scenario,
    const RecordingBoundarySelection& selection,
    const std::unordered_map<RecordingPlanEdge, size_t, RecordingPlanEdgeHash>& edgeIndexByPlanEdge)
{
    std::vector<Json> serializedSelections;
    serializedSelections.reserve(selection.selectedBoundary.size());
    for (const auto& selectedBoundary : selection.selectedBoundary)
    {
        const auto edgeIndexIt = edgeIndexByPlanEdge.find(selectedBoundary.candidate.edge);
        if (edgeIndexIt == edgeIndexByPlanEdge.end())
        {
            throw std::runtime_error("selection references an unknown boundary edge");
        }

        const auto selectionHostIndex = findHostIndex(scenario.hosts, selectedBoundary.chosenOption.selection.node);
        serializedSelections.push_back(Json{
            {"edge_index", edgeIndexIt->second},
            {"decision", recordingDecisionName(selectedBoundary.chosenOption.decision)},
            {"selection_host_index", selectionHostIndex},
            {"selection_host_name", selectedBoundary.chosenOption.selection.node.getRawValue()},
            {"recording_id", selectedBoundary.chosenOption.selection.recordingId.getRawValue()},
            {"representation", recordingRepresentationName(selectedBoundary.chosenOption.selection.representation)},
            {"maintenance_cost", selectedBoundary.chosenOption.cost.maintenanceCost},
            {"incremental_storage_bytes", selectedBoundary.chosenOption.cost.incrementalStorageBytes},
            {"estimated_replay_latency_ms", selectedBoundary.chosenOption.cost.estimatedReplayLatencyMs},
            {"replay_time_multiplier", selectedBoundary.chosenOption.cost.replayTimeMultiplier}});
    }

    std::ranges::sort(serializedSelections, {}, [](const Json& selectionJson) { return selectionJson.at("edge_index").get<size_t>(); });

    Json serializedBoundary = Json::array();
    for (auto& selectionJson : serializedSelections)
    {
        serializedBoundary.push_back(std::move(selectionJson));
    }
    return serializedBoundary;
}

[[nodiscard]] Json serializeHosts(const ScenarioData& scenario)
{
    Json hosts = Json::array();
    for (size_t hostIndex = 0; hostIndex < scenario.hosts.size(); ++hostIndex)
    {
        hosts.push_back(Json{{"index", hostIndex}, {"name", scenario.hosts[hostIndex].getRawValue()}});
    }
    return hosts;
}

[[nodiscard]] Json serializeHostLinks(const ScenarioData& scenario)
{
    Json hostLinks = Json::array();
    for (size_t hostIndex = 0; hostIndex < scenario.hostNeighborsByIndex.size(); ++hostIndex)
    {
        for (const auto neighborIndex : scenario.hostNeighborsByIndex[hostIndex])
        {
            if (hostIndex < neighborIndex)
            {
                hostLinks.push_back(Json{{"from", hostIndex}, {"to", neighborIndex}});
            }
        }
    }
    return hostLinks;
}

[[nodiscard]] Json serializeNodes(const ScenarioData& scenario)
{
    Json nodes = Json::array();
    for (size_t nodeIndex = 0; nodeIndex < scenario.nodes.size(); ++nodeIndex)
    {
        const auto& node = scenario.nodes[nodeIndex];
        nodes.push_back(Json{
            {"index", nodeIndex},
            {"operator_id", node.id.getRawValue()},
            {"kind", node.kind},
            {"replay_time_ms", node.replayTimeMs},
            {"is_leaf", node.isLeaf}});
    }
    return nodes;
}

[[nodiscard]] Json serializeEdges(const ScenarioData& scenario)
{
    Json edges = Json::array();
    for (size_t edgeIndex = 0; edgeIndex < scenario.edges.size(); ++edgeIndex)
    {
        const auto& edge = scenario.edges[edgeIndex];
        Json routeHostIndices = Json::array();
        for (const auto& routeHost : edge.routeNodes)
        {
            routeHostIndices.push_back(findHostIndex(scenario.hosts, routeHost));
        }

        size_t feasibleOptionCount = 0;
        size_t optionCount = 0;
        if (edgeHasCandidate(edge))
        {
            const auto& candidate = requireCandidate(edge);
            optionCount = candidate.options.size();
            for (const auto& option : candidate.options)
            {
                if (option.feasible)
                {
                    ++feasibleOptionCount;
                }
            }
        }

        edges.push_back(Json{
            {"index", edgeIndex},
            {"parent_node_index", edge.parentNodeIndex},
            {"child_node_index", edge.childNodeIndex},
            {"route_host_indices", std::move(routeHostIndices)},
            {"option_count", optionCount},
            {"feasible_option_count", feasibleOptionCount}});
    }
    return edges;
}

[[nodiscard]] Json buildReportInstanceJson(
    const ScenarioData& scenario,
    const FrozenScenario& frozenScenario,
    const RecordingBoundarySelection& solverSelection,
    const CutMetrics& solverMetrics,
    const double solverElapsedNs,
    const std::optional<SelectionWithMetrics>& exactBaseline,
    const std::optional<ScenarioStats>& exactStats,
    const std::optional<size_t>& selectedRank,
    const std::optional<double>& selectedOverBest)
{
    const auto optionSummary = summarizeCandidateOptions(scenario);
    const auto edgeIndexByPlanEdge = buildEdgeIndexMap(scenario);

    Json instance = Json{
        {"instance_id", frozenScenario.instanceId},
        {"scenario", scenario.scenarioName},
        {"baseline_mode", frozenScenario.baselineMode},
        {"network_topology", frozenScenario.networkTopology},
        {"corpus_bucket", frozenScenario.corpusBucket},
        {"budget_profile", frozenScenario.budgetProfile},
        {"seed", frozenScenario.seed},
        {"tree_height", frozenScenario.treeHeight},
        {"fanout", frozenScenario.fanout},
        {"host_count", scenario.hosts.size()},
        {"node_count", scenario.nodes.size()},
        {"candidate_edges", optionSummary.candidateEdges},
        {"candidate_options", optionSummary.candidateOptions},
        {"reuse_options", optionSummary.reuseOptions},
        {"upgrade_options", optionSummary.upgradeOptions},
        {"create_options", optionSummary.createOptions},
        {"replay_latency_limit_ms", scenario.replayLatencyLimitMs},
        {"storage_budgets_bytes", scenario.storageBudgetsBytes},
        {"constraints_bind", frozenScenario.constraintsBind},
        {"feasible_fraction", frozenScenario.feasibleFraction},
        {"latency_quantile", frozenScenario.latencyQuantile},
        {"storage_quantile", frozenScenario.storageQuantile},
        {"hosts", serializeHosts(scenario)},
        {"host_links", serializeHostLinks(scenario)},
        {"nodes", serializeNodes(scenario)},
        {"edges", serializeEdges(scenario)},
        {"solver_boundary", serializeBoundarySelection(scenario, solverSelection, edgeIndexByPlanEdge)},
        {"solver_metrics",
         Json{
             {"elapsed_ns", solverElapsedNs},
             {"objective_cost", solverSelection.objectiveCost},
             {"selected_edge_count", solverMetrics.selectedEdgeCount},
             {"maintenance_cost", solverMetrics.maintenanceCost},
             {"replay_time_ms", solverMetrics.replayTimeMs}}}};

    if (exactBaseline.has_value() && exactStats.has_value() && selectedRank.has_value() && selectedOverBest.has_value())
    {
        instance["exact_baseline_boundary"] = serializeBoundarySelection(scenario, exactBaseline->selection, edgeIndexByPlanEdge);
        instance["exact_baseline_metrics"] = Json{
            {"objective_cost", exactBaseline->selection.objectiveCost},
            {"selected_edge_count", exactBaseline->metrics.selectedEdgeCount},
            {"maintenance_cost", exactBaseline->metrics.maintenanceCost},
            {"replay_time_ms", exactBaseline->metrics.replayTimeMs},
            {"valid_cut_count", exactStats->validCutCount},
            {"feasible_cut_count", exactStats->feasibleCutCount},
            {"best_feasible_maintenance_cost", exactStats->bestMaintenanceCost},
            {"best_feasible_replay_time_ms", exactStats->bestReplayTimeMs}};
        instance["selected_rank"] = *selectedRank;
        instance["selected_over_best"] = *selectedOverBest;
    }

    return instance;
}

void writeReportFile(const std::vector<FrozenScenario>& frozenCorpus, const std::string& outputPath)
{
    Json root;
    root["schema_version"] = REPORT_SCHEMA_VERSION;
    root["instances"] = Json::array();
    root["instance_count"] = frozenCorpus.size();

    std::unordered_map<std::string, BaseScenarioCacheEntry> baseScenarioCache;
    size_t exactInstanceCount = 0;
    size_t performanceOnlyInstanceCount = 0;
    for (const auto& frozenScenario : frozenCorpus)
    {
        const auto cacheKey = baseScenarioKey(frozenScenario);
        ensureBaseScenarioCacheEntry(frozenScenario, baseScenarioCache);

        const auto& cacheEntry = baseScenarioCache.at(cacheKey);
        auto scenario = cacheEntry.baseScenario;
        if (scenario.hosts.size() != frozenScenario.storageBudgetsBytes.size())
        {
            throw std::runtime_error("frozen corpus instance has mismatched storage budget vector: " + frozenScenario.instanceId);
        }
        scenario.storageBudgetsBytes = frozenScenario.storageBudgetsBytes;
        scenario.replayLatencyLimitMs = frozenScenario.replayLatencyLimitMs;
        scenario.candidateSet.replayLatencyLimitMs = scenario.replayLatencyLimitMs;

        const auto workerCatalog = makeWorkerCatalog(scenario);
        const auto constWorkerCatalog = makeConstWorkerCatalog(workerCatalog);
        const auto solverResult = runSolverOnce(scenario.candidateSet, constWorkerCatalog, makeShadowPriceTuning());
        if (!solverResult.succeeded || !solverResult.selection.has_value())
        {
            throw std::runtime_error("failed to compute solver selection for report instance " + frozenScenario.instanceId);
        }

        size_t visitedSelectedEdges = 0;
        const auto solverMetrics = evaluateSelectionMetrics(scenario, *solverResult.selection, visitedSelectedEdges);
        if (visitedSelectedEdges != solverResult.selection->selectedBoundary.size())
        {
            throw std::runtime_error("solver returned a non-boundary cut while exporting report data");
        }
        if (!isFeasibleCut(solverMetrics, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes))
        {
            throw std::runtime_error("solver returned an infeasible selection while exporting report data");
        }

        if (hasExactBaseline(parseBaselineMode(frozenScenario.baselineMode)))
        {
            const auto exactStats = computeFeasibleCutStats(cacheEntry.allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
            verifyExactFrozenScenario(frozenScenario, exactStats, cacheEntry.unconstrainedBest, scenario);
            auto exactBaseline = computeBestExactSelection(scenario, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes);
            if (!almostEqual(exactBaseline.metrics.maintenanceCost, exactStats.bestMaintenanceCost)
                || !almostEqual(exactBaseline.metrics.replayTimeMs, exactStats.bestReplayTimeMs))
            {
                throw std::runtime_error("exact baseline selection drift detected for " + frozenScenario.instanceId);
            }
            exactBaseline.selection.objectiveCost = exactBaseline.metrics.maintenanceCost;
            auto solverSelection = *solverResult.selection;
            solverSelection.objectiveCost = solverMetrics.maintenanceCost;
            root["instances"].push_back(
                buildReportInstanceJson(
                    scenario,
                    frozenScenario,
                    solverSelection,
                    solverMetrics,
                    solverResult.elapsedNs,
                    exactBaseline,
                    exactStats,
                    computeSelectionRank(solverMetrics, cacheEntry.allCuts, scenario.replayLatencyLimitMs, scenario.storageBudgetsBytes),
                    positiveRatio(solverMetrics.maintenanceCost, exactStats.bestMaintenanceCost)));
            ++exactInstanceCount;
            continue;
        }

        verifyPerformanceFrozenScenario(frozenScenario, cacheEntry.unconstrainedBest, scenario);
        auto solverSelection = *solverResult.selection;
        solverSelection.objectiveCost = solverMetrics.maintenanceCost;
        root["instances"].push_back(
            buildReportInstanceJson(
                scenario,
                frozenScenario,
                solverSelection,
                solverMetrics,
                solverResult.elapsedNs,
                std::nullopt,
                std::nullopt,
                std::nullopt,
                std::nullopt));
        ++performanceOnlyInstanceCount;
    }

    root["exact_instance_count"] = exactInstanceCount;
    root["performance_only_instance_count"] = performanceOnlyInstanceCount;

    std::ofstream outputFile(outputPath, std::ios::out | std::ios::trunc);
    if (!outputFile.is_open())
    {
        throw std::runtime_error("failed to open report output file: " + outputPath);
    }
    outputFile << root.dump(2) << '\n';
}

void writeConvergenceCsvHeader(std::ostream& out)
{
    out << "scenario,baseline_mode,network_topology,instance_id,corpus_bucket,budget_profile,seed,tree_height,fanout,host_count,node_count,"
           "candidate_edges,candidate_options,iteration,constraints_bind,feasible_fraction,replay_initial_price_scale,"
           "replay_step_scale,storage_step_scale,shadow_price_iterations,solver_succeeded,"
           "replay_time_price,selected_replay_time_ms,replay_constraint_satisfaction_percent,"
           "storage_budget_constraint_satisfaction_percent,max_storage_utilization_percent,replay_constraint_satisfied,"
           "storage_budget_constraint_satisfied,current_iterate_feasible,returnable_solution_feasible\n";
}

void writeConvergenceCsvRow(std::ostream& out, const ConvergenceTraceRow& row)
{
    out << row.scenario << ',' << row.baselineMode << ',' << row.networkTopology << ',' << row.instanceId << ',' << row.corpusBucket << ',' << row.budgetProfile << ','
        << row.seed << ',' << row.treeHeight << ',' << row.fanout << ',' << row.hostCount << ',' << row.nodeCount << ','
        << row.candidateEdges << ',' << row.candidateOptions << ',' << row.iteration << ','
        << (row.constraintsBind ? 1 : 0) << ',' << row.feasibleFraction << ','
        << row.replayInitialPriceScale << ',' << row.replayStepScale << ',' << row.storageStepScale << ','
        << row.shadowPriceIterations << ',' << (row.solverSucceeded ? 1 : 0) << ',' << row.replayTimePrice << ','
        << row.selectedReplayTimeMs << ',' << row.replayConstraintSatisfactionPercent << ','
        << row.storageBudgetConstraintSatisfactionPercent << ',' << row.maxStorageUtilizationPercent << ','
        << (row.replayConstraintSatisfied ? 1 : 0) << ',' << (row.storageBudgetConstraintSatisfied ? 1 : 0) << ','
        << (row.currentIterateFeasible ? 1 : 0) << ',' << (row.returnableSolutionFeasible ? 1 : 0) << '\n';
}

void writeCsvHeader(std::ostream& out)
{
    out << "scenario,baseline_mode,network_topology,seed,tree_height,fanout,host_count,node_count,candidate_edges,candidate_options,"
           "reuse_options,upgrade_options,create_options,leaf_count,valid_cut_count,feasible_cut_count,"
           "replay_latency_limit_ms,selected_edge_count,selected_maintenance_cost,selected_replay_time_ms,"
           "best_feasible_maintenance_cost,best_feasible_replay_time_ms,avg_feasible_maintenance_cost,"
           "worst_feasible_maintenance_cost,selected_over_best,avg_over_best,worst_over_best,selected_rank,solver_time_ns_mean,"
           "benchmark_mode,instance_id,corpus_bucket,budget_profile,feasible_fraction,latency_quantile,storage_quantile,"
           "unconstrained_best_maintenance_cost,unconstrained_best_replay_time_ms,constraints_bind,replay_initial_price_scale,"
           "replay_step_scale,storage_step_scale,shadow_price_iterations,solver_succeeded,optimal_hit\n";
}

void writeCsvRow(std::ostream& out, const ScenarioRow& row)
{
    out << row.scenario << ',' << row.baselineMode << ',' << row.networkTopology << ',' << row.seed << ',' << row.treeHeight << ',' << row.fanout << ','
        << row.hostCount << ',' << row.nodeCount << ',' << row.candidateEdges << ',' << row.candidateOptions << ','
        << row.reuseOptions << ',' << row.upgradeOptions << ',' << row.createOptions << ',' << row.leafCount << ','
        << row.validCutCount << ',' << row.feasibleCutCount << ',' << row.replayLatencyLimitMs << ','
        << row.selectedEdgeCount << ',' << row.selectedMaintenanceCost << ',' << row.selectedReplayTimeMs << ','
        << row.bestMaintenanceCost << ',' << row.bestReplayTimeMs << ',' << row.averageMaintenanceCost << ','
        << row.worstMaintenanceCost << ',' << row.selectedOverBest << ',' << row.averageOverBest << ',' << row.worstOverBest
        << ',' << row.selectedRank << ',' << row.solverTimeNsMean << ',' << row.benchmarkMode << ',' << row.instanceId << ','
        << row.corpusBucket << ',' << row.budgetProfile << ',' << row.feasibleFraction << ',' << row.latencyQuantile << ','
        << row.storageQuantile << ',' << row.unconstrainedBestMaintenanceCost << ',' << row.unconstrainedBestReplayTimeMs << ','
        << (row.constraintsBind ? 1 : 0) << ',' << row.replayInitialPriceScale << ',' << row.replayStepScale << ','
        << row.storageStepScale << ',' << row.shadowPriceIterations << ',' << (row.solverSucceeded ? 1 : 0) << ','
        << (row.optimalHit ? 1 : 0) << '\n';
}

int runBenchmark(const BenchmarkConfig& config)
{
    std::vector<FrozenScenario> frozenCorpus;
    if (!config.legacyAdaptive)
    {
        frozenCorpus = config.corpusInputPath.has_value() ? readFrozenCorpusFile(*config.corpusInputPath) : generateFrozenCorpus(config);
        if (config.corpusOutputPath.has_value())
        {
            writeFrozenCorpusFile(frozenCorpus, *config.corpusOutputPath);
        }
        if (config.corpusOnly)
        {
            return 0;
        }
        if (config.reportOutputPath.has_value())
        {
            writeReportFile(frozenCorpus, *config.reportOutputPath);
            if (!config.outputPath.has_value())
            {
                return 0;
            }
        }
    }

    std::ofstream outputFile;
    std::ostream* output = &std::cout;
    if (config.outputPath.has_value())
    {
        outputFile.open(*config.outputPath, std::ios::out | std::ios::trunc);
        if (!outputFile.is_open())
        {
            throw std::runtime_error("failed to open output file: " + *config.outputPath);
        }
        output = &outputFile;
    }

    output->setf(std::ios::fixed);
    output->precision(6);
    if (config.shadowPriceConvergence)
    {
        writeConvergenceCsvHeader(*output);
        std::unordered_map<std::string, BaseScenarioCacheEntry> baseScenarioCache;
        size_t writtenRows = 0;
        for (const auto& frozenScenario : frozenCorpus)
        {
            if (!config.includeNonbindingSweepScenarios && !frozenScenario.constraintsBind)
            {
                continue;
            }
            for (const auto& row : runFrozenScenarioShadowPriceConvergence(config, frozenScenario, baseScenarioCache))
            {
                writeConvergenceCsvRow(*output, row);
                ++writtenRows;
            }
        }
        if (writtenRows == 0)
        {
            throw std::runtime_error(
                "shadow-price convergence trace did not emit any rows; try --include-nonbinding-scenarios or a different corpus");
        }
        return 0;
    }

    writeCsvHeader(*output);
    if (config.legacyAdaptive)
    {
        for (size_t fanout = config.minFanout; fanout <= config.maxFanout; ++fanout)
        {
            for (uint64_t seed = config.firstSeed; seed < config.firstSeed + config.seedCount; ++seed)
            {
                writeCsvRow(*output, runAdaptiveScenario(config, fanout, seed));
            }
        }
        return 0;
    }

    std::unordered_map<std::string, BaseScenarioCacheEntry> baseScenarioCache;
    if (config.shadowPriceSweep)
    {
        size_t writtenRows = 0;
        for (const auto& frozenScenario : frozenCorpus)
        {
            if (!config.includeNonbindingSweepScenarios && !frozenScenario.constraintsBind)
            {
                continue;
            }
            for (const auto replayInitialPriceScale : config.replayInitialPriceScales)
            {
                for (const auto replayStepScale : config.replayStepScales)
                {
                    writeCsvRow(
                        *output,
                        runFrozenScenarioShadowPriceSweep(
                            config,
                            frozenScenario,
                            replayInitialPriceScale,
                            replayStepScale,
                            baseScenarioCache));
                    ++writtenRows;
                }
            }
        }
        if (writtenRows == 0)
        {
            throw std::runtime_error("shadow-price sweep did not emit any rows; try --include-nonbinding-scenarios or a different corpus");
        }
        return 0;
    }

    for (const auto& frozenScenario : frozenCorpus)
    {
        writeCsvRow(*output, runFrozenScenario(config, frozenScenario, baseScenarioCache));
    }
    return 0;
}

} // namespace
} // namespace NES

int main(const int argc, char** argv)
{
    try
    {
        const auto config = NES::parseArguments(argc, argv);
        return NES::runBenchmark(config);
    }
    catch (const std::exception& exception)
    {
        std::cerr << "recording-selection-benchmark failed: " << exception.what() << '\n';
        return 1;
    }
}
