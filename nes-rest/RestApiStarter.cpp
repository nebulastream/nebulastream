#include <algorithm>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <httplib.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/EmbeddedWorkerQueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/base.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>

using json = nlohmann::json;
using namespace httplib;

struct NotFound
{
    NES::Exception inner;
};

struct BadRequest
{
    NES::Exception inner;
};

template <typename Ex, typename T>
static T valueOrThrow(std::expected<T, NES::Exception> value)
{
    if (!value.has_value())
    {
        throw Ex{value.error()};
    }
    return value.value();
}

static std::unordered_map<std::string, NES::ParserConfig> ParserConfigs
    = {{"CSV", NES::ParserConfig{.parserType = "CSV", .tupleDelimiter = ",", .fieldDelimiter = "\n"}}};

template <typename Ex, typename T, typename... Arguments>
static T valueOrThrow(std::optional<T> value, fmt::format_string<Arguments...> fmtSpec, Arguments&&... args)
{
    if (!value.has_value())
    {
        throw Ex{NES::UnknownException(fmtSpec, std::forward<Arguments>(args)...)};
    }
    return value.value();
}

enum class NodeStatus
{
    ONLINE,
    OFFLINE
};
// Convert enums to/from strings
static std::string queryStatusToString(NES::QueryStatus status)
{
    return std::string(magic_enum::enum_name(status));
}

static std::string nodeStatusToString(NodeStatus status)
{
    return std::string(magic_enum::enum_name(status));
}

static std::string fieldTypeToString(NES::DataType::Type type)
{
    return std::string(magic_enum::enum_name(type));
}

// Data structures
struct SchemaField
{
    static SchemaField fromJsonField(const json& j)
    {
        SchemaField field;
        field.fieldName = j["field_name"];
        try
        {
            field.fieldType = NES::DataTypeProvider::provideDataType(j["field_type"].template get<std::string>());
            return field;
        }
        catch (NES::Exception& e)
        {
            throw BadRequest(e);
        }
    }
    static std::vector<SchemaField> fromJson(const json& j)
    {
        std::vector<SchemaField> fields;
        for (const auto& field : j)
        {
            fields.emplace_back(fromJsonField(field));
        }
        return fields;
    }

    std::string fieldName;
    NES::DataType fieldType;

    [[nodiscard]] json toJson() const { return json{{"field_name", fieldName}, {"field_type", fieldTypeToString(fieldType.type)}}; }
};

struct TopologyLink
{
    int parent;
    int child;

    [[nodiscard]] json toJson() const { return json{{"parent", parent}, {"child", child}}; }
};

struct WorkerNode
{
    int workerId;
    NodeStatus status;

    [[nodiscard]] json toJson() const { return json{{"worker_id", workerId}, {"status", nodeStatusToString(status)}}; }
};

struct QueryPlanOperator
{
    int operatorId;
    std::string type;

    [[nodiscard]] json toJson() const { return json{{"operator_id", operatorId}, {"type", type}}; }
};

struct QueryPlanLink
{
    int source;
    int target;

    [[nodiscard]] json toJson() const { return json{{"source", source}, {"target", target}}; }
};

struct OperatorPlacement
{
    int operatorId;
    std::string type;
    int workerId;

    [[nodiscard]] json toJson() const { return json{{"operator_id", operatorId}, {"type", type}, {"worker_id", workerId}}; }
};

struct Topology
{
    std::vector<WorkerNode> topologyNodes;
    std::vector<TopologyLink> topologyEdges;

    [[nodiscard]] json toJson() const
    {
        json nodes = json::array();
        for (const auto& node : topologyNodes)
        {
            nodes.push_back(node.toJson());
        }

        json edges = json::array();
        for (const auto& edge : topologyEdges)
        {
            edges.push_back(edge.toJson());
        }

        return json{{"topology_nodes", nodes}, {"topology_edges", edges}};
    }
};

struct QueryPlan
{
    int queryId{};
    std::vector<QueryPlanOperator> nodes;
    std::vector<QueryPlanLink> edges;

    [[nodiscard]] json toJson() const
    {
        json nodeArray = json::array();
        for (const auto& node : nodes)
        {
            nodeArray.push_back(node.toJson());
        }

        json edgeArray = json::array();
        for (const auto& edge : edges)
        {
            edgeArray.push_back(edge.toJson());
        }

        return json{{"query_id", queryId}, {"nodes", nodeArray}, {"edges", edgeArray}};
    }
};

static QueryPlanOperator toGraphicalQueryPlanNode(const NES::LogicalOperator& logicalOperator)
{
    return QueryPlanOperator{
        .operatorId = static_cast<int>(logicalOperator.getId().getRawValue()),
        .type = logicalOperator.explain(NES::ExplainVerbosity::Short)};
}
static QueryPlan toGraphicalQueryPlan(NES::QueryId queryId, const NES::LogicalPlan& plan)
{
    QueryPlan result;
    result.queryId = queryId.getRawValue();
    std::vector<std::pair<int, NES::LogicalOperator>> stack;
    for (const auto& root : plan.getRootOperators())
    {
        stack.emplace_back(-1, root);
    }

    while (!stack.empty())
    {
        const auto [parent, current] = stack.back();
        stack.pop_back();
        auto node = toGraphicalQueryPlanNode(current);
        result.nodes.emplace_back(node);
        result.edges.emplace_back(parent, node.operatorId);

        for (const auto& child : current.getChildren())
        {
            stack.emplace_back(node.operatorId, child);
        }
    }

    return result;
}

struct Query
{
    NES::QueryId queryId;
    NES::QueryStatus status;
    std::string code;
    QueryPlan queryPlan;
    QueryPlan executionPlan;

    [[nodiscard]] json toJson() const
    {
        return json{
            {"query_id", queryId.getRawValue()},
            {"status", queryStatusToString(status)},
            {"code", code},
            {"query_plan", queryPlan.toJson()},
            {"execution_plan", executionPlan.toJson()}};
    }

    [[nodiscard]] json toOverviewJson() const
    {
        return json{{"query_id", queryId.getRawValue()}, {"status", queryStatusToString(status)}, {"code", code}};
    }
};

struct PhysicalSource
{
    NES::PhysicalSourceId physicalSource = NES::INVALID<NES::PhysicalSourceId>;
    int workerId{};
    std::string connector;
    std::unordered_map<std::string, std::string> config;
    std::string formatter;

    json toJson() const
    {
        return json{
            {"physical_source", physicalSource.getRawValue()},
            {"worker_id", workerId},
            {"connector", connector},
            {"config", config},
            {"formatter", formatter}};
    }
};

struct Source
{
    std::string sourceName;
    std::vector<SchemaField> schema;

    [[nodiscard]] json toJson() const
    {
        json schemaArray = json::array();
        for (const auto& field : schema)
        {
            schemaArray.push_back(field.toJson());
        }

        return json{{"source_name", sourceName}, {"schema", schemaArray}};
    }

    [[nodiscard]] json toOverviewJson() const { return json{{"source_name", sourceName}}; }
};

struct Sink
{
    std::string sinkName;
    int workerId{};
    std::string connector;
    std::unordered_map<std::string, std::string> config;

    json toJson() const { return json{{"sink_name", sinkName}, {"worker_id", workerId}, {"connector", connector}, {"config", config}}; }
};

// Global data storage (in production, use proper database)
class NebulaStreamData
{
    struct RegisteredQuery
    {
        std::string query;
        NES::LogicalPlan unoptimized;
        NES::LogicalPlan optimized;
    };

    std::shared_ptr<NES::SourceCatalog> sources = std::make_shared<NES::SourceCatalog>();
    std::shared_ptr<NES::SinkCatalog> sinks = std::make_shared<NES::SinkCatalog>();
    NES::EmbeddedWorkerQueryManager qm = NES::EmbeddedWorkerQueryManager(NES::SingleNodeWorkerConfiguration{});
    std::unordered_map<NES::QueryId, RegisteredQuery> queries;

public:
    // Query operations
    NES::QueryId addQuery(const std::string& code)
    {
        auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(code);
        const NES::CLI::LegacyOptimizer optimizer(sources, sinks);
        auto optimizedPlan = optimizer.optimize(plan);
        auto id = valueOrThrow<BadRequest>(qm.registerQuery(optimizedPlan));
        valueOrThrow<BadRequest>(qm.start(id));
        queries.emplace(id, RegisteredQuery{.query = code, .unoptimized = plan, .optimized = optimizedPlan});
        return id;
    }

    std::vector<Query> getAllQueries()
    {
        std::vector<Query> result;
        for (const auto& [qid, q] : queries)
        {
            auto summary = valueOrThrow<NotFound>(qm.status(qid));
            result.emplace_back(Query{
                .queryId = summary.queryId,
                .status = summary.currentStatus,
                .code = q.query,
                .queryPlan = toGraphicalQueryPlan(qid, q.unoptimized),
                .executionPlan = toGraphicalQueryPlan(qid, q.optimized)});
        }
        return result;
    }

    Query getQuery(int id)
    {
        const NES::QueryId qid{static_cast<NES::QueryId::Underlying>(id)};
        auto it = queries.find(qid);
        if (it == queries.end())
        {
            throw "Not Found";
        }

        auto summary = valueOrThrow<NotFound>(qm.status(qid));
        return Query{
            .queryId = summary.queryId,
            .status = summary.currentStatus,
            .code = it->second.query,
            .queryPlan = toGraphicalQueryPlan(qid, it->second.unoptimized),
            .executionPlan = toGraphicalQueryPlan(qid, it->second.optimized)};
    }

    void deleteQuery(int id)
    {
        const NES::QueryId qid{static_cast<NES::QueryId::Underlying>(id)};
        valueOrThrow<NotFound>(qm.stop(qid));
        qm.unregister(qid);
        queries.erase(qid);
    }

    // Source operations
    void addSource(const Source& source)
    {
        NES::Schema schema;
        for (const auto& schemaField : source.schema)
        {
            schema = schema.addField(schemaField.fieldName, schemaField.fieldType);
        }

        auto _ = valueOrThrow<BadRequest>(
            sources->addLogicalSource(source.sourceName, schema), "LogicalSource '{}' already exists", source.sourceName);
    }

    PhysicalSource addPhysicalSource(const std::string& logicalSourceName, const PhysicalSource& source)
    {
        auto logicalSource
            = valueOrThrow<NotFound>(sources->getLogicalSource(logicalSourceName), "Logical source '{}' does not exist", logicalSourceName);

        auto parserType = ParserConfigs.find(source.formatter);
        if (parserType == ParserConfigs.end())
        {
            throw BadRequest{NES::UnknownParserType("'{}'", source.formatter)};
        }

        auto addedSource = valueOrThrow<BadRequest>(
            sources->addPhysicalSource(logicalSource, source.connector, source.config, parserType->second), "Schema already exists");

        auto copy = source;
        copy.physicalSource = addedSource.getPhysicalSourceId();
        return copy;
    }

    std::vector<Source> getAllSources()
    {
        std::vector<Source> result;
        for (const auto& s : sources->getAllLogicalSources())
        {
            result.emplace_back(Source{
                .sourceName = s.getLogicalSourceName(),
                .schema = std::views::transform(
                              s.getSchema()->getFields(), [](const auto& field) { return SchemaField{field.name, field.dataType}; })
                    | std::ranges::to<std::vector<SchemaField>>()});
        }

        return result;
    }

    Source getSource(const std::string& name)
    {
        auto source = valueOrThrow<NotFound>(sources->getLogicalSource(name), "Logical source '{}' does not exist", name);

        return Source{
            .sourceName = source.getLogicalSourceName(),
            .schema = std::views::transform(
                          source.getSchema()->getFields(), [](const auto& field) { return SchemaField{field.name, field.dataType}; })
                | std::ranges::to<std::vector<SchemaField>>()};
    }

    std::vector<PhysicalSource> getPhysicalSources(const std::string& sourceName)
    {
        auto logicalSource
            = valueOrThrow<NotFound>(sources->getLogicalSource(sourceName), "Logical source '{}' does not exist", sourceName);
        auto physicalSources = sources->getPhysicalSources(logicalSource).value();

        return std::views::transform(
                   physicalSources,
                   [](const NES::SourceDescriptor& descriptor)
                   {
                       return PhysicalSource{
                           .physicalSource = descriptor.getPhysicalSourceId(),
                           .workerId = 0,
                           .connector = descriptor.getSourceType(),
                           .config = NES::DescriptorConfig::toStringConfig(descriptor.getConfig()),
                           .formatter = descriptor.getParserConfig().parserType};
                   })
            | std::ranges::to<std::vector>();
    }

    std::vector<PhysicalSource> getAllPhysicalSources()
    {
        std::vector<PhysicalSource> result;
        for (const auto& s : sources->getAllLogicalSources())
        {
            auto physicalSources = sources->getPhysicalSources(s).value();
            std::ranges::copy(
                std::views::transform(
                    physicalSources,
                    [](const NES::SourceDescriptor& descriptor)
                    {
                        return PhysicalSource{
                            .physicalSource = descriptor.getPhysicalSourceId(),
                            .workerId = 0,
                            .connector = descriptor.getSourceType(),
                            .config = NES::DescriptorConfig::toStringConfig(descriptor.getConfig()),
                            .formatter = descriptor.getParserConfig().parserType};
                    }),
                std::back_inserter(result));
        }

        return result;
    }

    // Sink operations
    void addSink(const Sink& sink)
    {
        const NES::Schema schema{};
        auto _ = valueOrThrow<BadRequest>(sinks->addSinkDescriptor(sink.sinkName, schema, sink.connector, sink.config));
    }

    std::vector<Sink> getAllSinks()
    {
        auto sinkDescriptors = sinks->getAllSinkDescriptors();
        std::vector<Sink> result;
        result.reserve(sinkDescriptors.size());
        for (const auto& sinkDescriptor : sinkDescriptors)
        {
            result.emplace_back(Sink{
                .sinkName = sinkDescriptor.getSinkName(),
                .workerId = 0,
                .connector = sinkDescriptor.getSinkType(),
                .config = NES::DescriptorConfig::toStringConfig(sinkDescriptor.getConfig())});
        }
        return result;
    }

    Sink getSink(const std::string& name)
    {
        auto sinkDescriptor = valueOrThrow<NotFound>(sinks->getSinkDescriptor(name), "Sink '{}' does not exist", name);
        return Sink{
            .sinkName = sinkDescriptor.getSinkName(),
            .workerId = 0,
            .connector = sinkDescriptor.getSinkType(),
            .config = NES::DescriptorConfig::toStringConfig(sinkDescriptor.getConfig())};
    }

    // Topology operations
    static void addWorkerNode(const WorkerNode&) { throw "Not implemented"; }

    static std::vector<WorkerNode> getAllWorkerNodes() { return {WorkerNode{.workerId = 0, .status = NodeStatus::ONLINE}}; }

    static WorkerNode getWorkerNode(int) { return WorkerNode{.workerId = 0, .status = NodeStatus::ONLINE}; }

    Topology getTopology()
    {
        Topology topology;
        topology.topologyNodes = getAllWorkerNodes();
        return topology;
    }
};

// Global instance
static NebulaStreamData g_data;

// Error response helper
static json createErrorResponse(const std::string& message)
{
    return json{{"message", message}};
}


static void requestCommon(const Request& req, Response& res, const std::function<void(const Request&, Response&)>& handler)
{
    try
    {
        NES_INFO("Starting request {}", req.path);
        handler(req, res);
        INVARIANT(res.status != 0, "Response status is not set");
        NES_INFO("Request was successfully: {}", res.body);
        return;
    }
    catch (const NotFound& nf)
    {
        res.status = 404;
        res.set_content(createErrorResponse(nf.inner.what()).dump(), "application/json");
    }
    catch (const BadRequest& nf)
    {
        res.status = 400;
        res.set_content(createErrorResponse(nf.inner.what()).dump(), "application/json");
    }
    catch (...)
    {
        NES::tryLogCurrentException();
        res.status = 500;
        res.set_content(createErrorResponse("Internal server error").dump(), "application/json");
    }
    NES_WARNING("Request was not successful");
}

// REST API Implementation
class NebulaStreamAPI
{
public:
    static void setupRoutes(Server& server)
    {
        // Health Check
        server.Get(
            "/api/v1/healthCheck",
            [](const Request&, Response& res)
            {
                const json response = {{"status", "OK"}};
                res.set_content(response.dump(), "application/json");
            });

        // Queries endpoints
        server.Post(
            "/api/v1/queries",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        auto body = json::parse(req.body);
                        const std::string code = body["code"];

                        auto queryId = g_data.addQuery(code);
                        const json response = {{"query_id", queryId.getRawValue()}, {"status", "REGISTERED"}};

                        res.status = 201;
                        res.set_content(response.dump(), "application/json");
                    });
            });

        server.Get(
            "/api/v1/queries",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const auto&, Response& res)
                    {
                        auto queries = g_data.getAllQueries();
                        json queriesArray = json::array();

                        for (const auto& query : queries)
                        {
                            queriesArray.push_back(query.toOverviewJson());
                        }

                        const json response = {{"queries", queriesArray}};
                        res.set_content(response.dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/queries/(\d+))",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const int queryId = std::stoi(req.matches[1]);
                        const Query query = g_data.getQuery(queryId);
                        res.set_content(query.toJson().dump(), "application/json");
                    });
            });

        server.Delete(
            R"(/api/v1/queries/(\d+))",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        int queryId = std::stoi(req.matches[1]);

                        g_data.deleteQuery(queryId);
                        const json response = {{"query_id", queryId}, {"status", "STOPPED"}};
                        res.status = 202;
                        res.set_content(response.dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/queries/(\d+)/plan)",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const int queryId = std::stoi(req.matches[1]);
                        const Query query = g_data.getQuery(queryId);

                        res.set_content(query.queryPlan.toJson().dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/queries/(\d+)/execution)",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const int queryId = std::stoi(req.matches[1]);
                        const Query query = g_data.getQuery(queryId);

                        res.set_content(query.executionPlan.toJson().dump(), "application/json");
                    });
            });

        // Sources endpoints
        server.Post(
            "/api/v1/sources",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        auto body = json::parse(req.body);

                        Source source;
                        source.sourceName = body["source_name"];
                        source.schema = SchemaField::fromJson(body["schema"]);

                        g_data.addSource(source);

                        res.status = 201;
                        res.set_content(source.toJson().dump(), "application/json");
                    });
            });

        server.Get(
            "/api/v1/sources",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const auto&, Response& res)
                    {
                        auto sources = g_data.getAllSources();
                        json sourcesArray = json::array();

                        for (const auto& source : sources)
                        {
                            sourcesArray.push_back(source.toOverviewJson());
                        }

                        const json response = {{"sources", sourcesArray}};
                        res.set_content(response.dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/sources/([^/]+))",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const std::string sourceName = req.matches[1];
                        const Source source = g_data.getSource(sourceName);
                        res.set_content(source.toJson().dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/sources/([^/]+)/physical)",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        std::string sourceName = req.matches[1];
                        auto physicalSources = g_data.getPhysicalSources(sourceName);

                        json sourcesArray = json::array();
                        for (const auto& ps : physicalSources)
                        {
                            sourcesArray.push_back(ps.toJson());
                        }

                        const json response = {{"logical_source", sourceName}, {"physical_sources", sourcesArray}};
                        res.set_content(response.dump(), "application/json");
                    });
            });


        server.Post(
            R"(/api/v1/sources/([^/]+)/physical)",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const std::string sourceName = req.matches[1];

                        auto body = json::parse(req.body);
                        PhysicalSource source;
                        source.workerId = body["worker_id"];
                        source.connector = body["connector"];
                        source.formatter = body["formatter"];
                        source.config = body["config"];

                        auto newSource = g_data.addPhysicalSource(sourceName, source);

                        res.status = 201;
                        res.set_content(newSource.toJson().dump(), "application/json");
                    });
            });

        // Sinks endpoints
        server.Get(
            "/api/v1/sinks",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const auto&, Response& res)
                    {
                        auto sinks = g_data.getAllSinks();
                        json sinksArray = json::array();

                        for (const auto& sink : sinks)
                        {
                            sinksArray.push_back(sink.toJson());
                        }

                        const json response = {{"sinks", sinksArray}};
                        res.status = 200;
                        res.set_content(response.dump(), "application/json");
                    });
            });

        // Sinks endpoints
        server.Post(
            "/api/v1/sinks",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        auto body = json::parse(req.body);

                        Sink sink;
                        sink.sinkName = body["sink_name"];
                        sink.workerId = body["worker_id"];
                        sink.connector = body["connector"];
                        sink.config = body["config"];

                        g_data.addSink(sink);

                        res.status = 201;
                        res.set_content(sink.toJson().dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/sinks/([^/]+))",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const std::string sinkName = req.matches[1];
                        const Sink sink = g_data.getSink(sinkName);
                        res.set_content(sink.toJson().dump(), "application/json");
                    });
            });

        // Topology endpoints
        server.Get(
            "/api/v1/topology",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const auto&, Response& res)
                    {
                        auto topology = g_data.getTopology();
                        res.set_content(topology.toJson().dump(), "application/json");
                    });
            });

        server.Get(
            "/api/v1/topology/nodes",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const auto&, Response& res)
                    {
                        auto nodes = g_data.getAllWorkerNodes();
                        json nodesArray = json::array();

                        for (const auto& node : nodes)
                        {
                            nodesArray.push_back(node.toJson());
                        }

                        const json response = {{"worker_nodes", nodesArray}};
                        res.set_content(response.dump(), "application/json");
                    });
            });

        server.Get(
            R"(/api/v1/topology/nodes/(\d+))",
            [](const Request& req, Response& res)
            {
                requestCommon(
                    req,
                    res,
                    [](const Request& req, Response& res)
                    {
                        const int workerId = std::stoi(req.matches[1]);
                        const WorkerNode node = g_data.getWorkerNode(workerId);

                        res.set_content(node.toJson().dump(), "application/json");
                    });
            });
    }
};

int main()
{
    NES::Logger::setupLogging("restapi.log", NES::LogLevel::LOG_DEBUG, true);
    const NES::SingleNodeWorkerConfiguration configuration;
    const NES::EmbeddedWorkerQueryManager queryManager(configuration);

    Server server;
    // Setup CORS headers
    server.set_pre_routing_handler(
        [](const Request&, Response& res)
        {
            res.set_header("Access-Control-Allow-Origin", "*");
            res.set_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
            res.set_header("Access-Control-Allow-Headers", "Content-Type");
            return Server::HandlerResponse::Unhandled;
        });

    server.Options(".*", [](const Request&, Response&) { });

    // Setup routes
    NebulaStreamAPI::setupRoutes(server);

    NES_INFO("NebulaStream REST API server starting on http://localhost:8081")

    server.listen("0.0.0.0", 8081);

    return 0;
}
