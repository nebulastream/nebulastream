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

#include <YAMLModel.h>
#include <argumentum/argparse.h>
#include <iostream>
#include <ranges>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>

static std::ostream* os;
namespace internal {
template<typename T>
struct GetTypeNameHelper {
    static std::string ExtractTypeName(const std::string& input) {
        size_t startPos = input.find("T = ");
        if (startPos == std::string::npos) {
            return "";// TYPE_NAME not found in input
        }

        startPos += 4;                            // Move to the start of TYPE_NAME
        size_t endPos = input.find("]", startPos);// Find the closing bracket

        if (endPos == std::string::npos) {
            return "";// Closing bracket not found
        }

        return input.substr(startPos, endPos - startPos);
    }
    static std::string GetTypeName() { return ExtractTypeName(__PRETTY_FUNCTION__); }
};
}// namespace internal

template<typename T>
std::string GetTypeName() {
    return internal::GetTypeNameHelper<T>::GetTypeName();
}

class HeaderGuard {
    std::string guardName;

  public:
    explicit HeaderGuard(const std::string& guardName) : guardName(guardName) {
        *os << "#ifndef " << guardName << std::endl;
        *os << "#define " << guardName << std::endl;
    }

    virtual ~HeaderGuard() { *os << "#endif //" << guardName << std::endl; }
};

class Include {
    std::string path;

  public:
    explicit Include(const std::string& path) : path(path) { *os << "#include <" << path << ">" << std::endl; }
};
class Namespace {
  public:
    explicit Namespace(const std::string& name) : name(name) { *os << "namespace " << name << " {" << std::endl; }

    virtual ~Namespace() { *os << "} //" << name << std::endl; }

  private:
    std::string name;
};

class Struct {
  public:
    explicit Struct(const std::string& name) : name(name) { *os << "struct " << name << " {" << std::endl; }

    virtual ~Struct() { *os << "};" << std::endl; }

  private:
    std::string name;
};

template<typename T>
class ConstExprStatic {
  public:
    ConstExprStatic(const std::string& name, T value) {
        *os << "constexpr static " << GetTypeName<T>() << " " << name << " = " << value << ";" << std::endl;
    }
};

template<>
ConstExprStatic<const char*>::ConstExprStatic(const std::string& name, const char* value) {
    *os << "constexpr static " << GetTypeName<const char*>() << " " << name << " = \"" << value << "\";" << std::endl;
}

class Source {
    size_t id;

  public:
    Source(QueryConfiguration queryConfig, WorkerLinkConfiguration link) {
        id = link.operatorId;
        {
            Struct s("SourceConfig" + std::to_string(link.operatorId));
            ConstExprStatic("QueryID", queryConfig.queryID);
            ConstExprStatic("UpstreamNodeID", link.nodeId);
            ConstExprStatic("UpstreamPartitionID", link.partitionId);
            ConstExprStatic("UpstreamSubPartitionID", link.subpartitionId);
            ConstExprStatic<const char*>("UpstreamNodeHostname", link.ip.c_str());
            ConstExprStatic("UpstreamOperatorID", link.operatorId);
            ConstExprStatic("UpstreamNodePort", link.port);
            ConstExprStatic<size_t>("LocalBuffers", 100);
        }
        *os << "using Source" << id << " = UnikernelSource<SourceConfig" << id << ">;" << std::endl;
    }

    void use() { *os << "Source" << id; }
};

class Sink {
    size_t id;

  public:
    Sink(QueryConfiguration queryConfig, WorkerLinkConfiguration link, NES::QuerySubPlanId subPlanId, size_t outputSchemaSizeInBytes) {
        id = link.operatorId;
        {
            Struct s("SinkConfig" + std::to_string(link.operatorId));
            ConstExprStatic("QueryID", queryConfig.queryID);
            ConstExprStatic("QuerySubplanID", subPlanId);
            ConstExprStatic("OutputSchemaSizeInBytes", outputSchemaSizeInBytes);
            ConstExprStatic("DownstreamNodeID", link.nodeId);
            ConstExprStatic("DownstreamPartitionID", link.partitionId);
            ConstExprStatic("DownstreamSubPartitionID", link.subpartitionId);
            ConstExprStatic<const char*>("DownstreamNodeHostname", link.ip.c_str());
            ConstExprStatic("DownstreamOperatorID", link.operatorId);
            ConstExprStatic("DownstreamNodePort", link.port);
            ConstExprStatic<size_t>("LocalBuffers", 100);
        }
        *os << "using Sink" << id << " = UnikernelSink<SinkConfig" << id << ">;" << std::endl;
    }

    void use() const { *os << "Sink" << id; }
};
class Stage {
    size_t stageId;

  public:
    explicit Stage(size_t stageId) : stageId(stageId) {}
    void use() const { *os << "Stage<" << stageId << ">"; }
};
class Pipeline {
    bool binary;
    std::optional<Pipeline*> leftPipeline;
    std::optional<Pipeline*> rightPipeline;

    std::optional<Pipeline*> nextPipeline;
    std::vector<Stage> stages;
    std::optional<Source> source;

    static bool isBinary(const WorkerStageConfiguration& stage) {
        return stage.predecessor.has_value() && stage.predecessor.value().size() > 1;
    }
    static bool isSource(const WorkerStageConfiguration& stage) { return stage.upstream.has_value(); }

  public:
    Pipeline(const QueryConfiguration& qc, const WorkerStageConfiguration& stage) {
        auto* current = &stage;
        while (true) {
            if (isBinary(*current) && stages.empty()) {
                this->binary = true;
                stages.emplace_back(current->stageId);
                leftPipeline.emplace(new Pipeline(qc, current->predecessor.value()[0]));
                rightPipeline.emplace(new Pipeline(qc, current->predecessor.value()[1]));
                break;
            } else if (isBinary(*current) && !stages.empty()) {
                nextPipeline.emplace(new Pipeline(qc, *current));
                break;
            } else if (isSource(*current)) {
                stages.emplace_back(current->stageId);
                source.emplace(qc, current->upstream.value());
                break;
            } else {
                stages.emplace_back(current->stageId);
                current = &current->predecessor.value()[0];
            }
        }
    }

    void use() {
        if (binary) {
            *os << "PipelineJoin<";
            stages[0].use();
            *os << ", ";
            leftPipeline.value()->use();
            *os << ", ";
            rightPipeline.value()->use();
            *os << ">";
        } else {
            if (stages.empty() && nextPipeline.has_value()) {
                nextPipeline.value()->use();
            } else if (stages.empty() && source.has_value()) {
                source.value().use();
            } else {
                assert(!stages.empty());
                *os << "Pipeline<";
                if (source.has_value()) {
                    source.value().use();
                    *os << ", ";
                }
                stages[0].use();
                for (auto stage : stages | std::ranges::views::drop(1)) {
                    *os << ", ";
                    stage.use();
                }
                if (nextPipeline.has_value()) {
                    *os << ", ";
                    nextPipeline.value()->use();
                }
                *os << ">";
            }
        }
    }

    virtual ~Pipeline() {
        if (nextPipeline.has_value()) {
            delete nextPipeline.value();
        }
    }
};
class SubQuery {
  public:
    size_t id;
    Sink sink;
    Pipeline pipeline;
    SubQuery(const QueryConfiguration& qc, const WorkerSubQueryConfiguration& sqc)
        : id(sqc.subQueryId), sink(qc, sqc.downstream, sqc.subQueryId, sqc.outputSchemaSizeInBytes), pipeline(qc, sqc.stages) {
        *os << "using SubQuery" << id << " = SubQuery<";
        sink.use();
        *os << ", ";
        pipeline.use();
        *os << ">;" << std::endl;
    }

    void use() const { *os << "SubQuery" << id; };
};
class QueryPlan {

  public:
    QueryPlan(const QueryConfiguration& qc, const WorkerConfiguration& wc) {
        std::vector<SubQuery> subQueries;
        for (const auto& item : wc.subQueries)
            subQueries.emplace_back(qc, item);

        *os << "using QueryPlan = UnikernelExecutionPlan<";
        for (const auto& item : subQueries)
            item.use();
        *os << ">;" << std::endl;
    }
};

int main(int argc, char** argv) {
    int workerId = 0;
    std::string configFilePath;
    std::string outputPath = "-";
    auto parser = argumentum::argument_parser{};
    auto params = parser.params();
    params.add_parameter(workerId, "ID").nargs(1).help("Worker Id");
    params.add_parameter(configFilePath, "-c", "--configPath").maxargs(1).help("Path to Config YAML");
    params.add_parameter(outputPath, "-o").maxargs(1).help("Path to output the generated Header File");
    parser.parse_args(argc, argv);

    std::ofstream ofs;
    if (outputPath.empty() || outputPath == "-") {
        os = &std::cout;
    } else {
        ofs = std::ofstream(outputPath);
        if (!ofs) {
            std::cerr << "Cannot Open Output file" << std::endl;
            return 0;
        }
        os = &ofs;
    }

    auto config = YAML::LoadFile(configFilePath).as<Configuration>();
    auto workerConfig = config.workers[workerId];

    HeaderGuard guard("NES_UNIKERNEL_PIPELINE_H");
    Include("UnikernelExecutionPlan.hpp");
    Include("UnikernelSink.h");
    Include("UnikernelSource.h");
    Include("UnikernelStage.h");
    Namespace ns("NES::Unikernel");
    {
        {
            Struct ctStruct("CTConfiguration");
            ConstExprStatic("QueryID", config.query.queryID);
            ConstExprStatic<size_t>("NodeID", workerConfig.nodeId);
            ConstExprStatic("NodeIP", workerConfig.ip.c_str());
            ConstExprStatic<size_t>("NodePort", workerConfig.port);
        }
        QueryPlan qp(config.query, workerConfig);
    }
}