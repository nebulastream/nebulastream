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

#include <Calcite/CalcitePlanner.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <unordered_set>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <google/protobuf/io/coded_stream.h>
#include <grpcpp/grpcpp.h>
#include <substrait/plan.pb.h>
#include <ErrorHandling.hpp>
#include <nes_calcite_planner.grpc.pb.h>

#ifdef NES_CALCITE_NATIVE
    #include <libnes-calcite-planner.h>
#endif

namespace NES::Calcite
{
namespace
{
/// NebulaStream optimizer rules whose rewrites Calcite performs (see Planner.java), not run again on Calcite plans.
constexpr std::array<std::string_view, 3> SKIPPED_NES_RULES
    = {"PredicatePushdownRule", "ProjectionPushdownRule", "RedundantProjectionRemovalRule"};

#ifdef NES_CALCITE_NATIVE
/// The planner compiled into this process with GraalVM (see nes-calcite/CMakeLists.txt); `address` is not used.
nes::calcite::PlanResponse callPlanner(const nes::calcite::PlanRequest& request, const std::string& /*address*/)
{
    /// One isolate (the native image's heap and runtime) for the whole process. Each thread that plans attaches to it once
    /// and stays attached.
    static graal_isolate_t* const isolate = []
    {
        graal_isolate_t* created = nullptr;
        graal_isolatethread_t* thread = nullptr;
        if (graal_create_isolate(nullptr, &created, &thread) != 0)
        {
            throw UnsupportedQuery("could not start the native Calcite planner");
        }
        return created;
    }();
    graal_isolatethread_t* thread = graal_get_current_thread(isolate);
    if (thread == nullptr and graal_attach_thread(isolate, &thread) != 0)
    {
        throw UnsupportedQuery("could not attach to the native Calcite planner");
    }

    auto serialized = request.SerializeAsString();
    char* responseData = nullptr;
    long long responseLength = 0; /// NOLINT(google-runtime-int): the C signature native-image generates
    if (nes_calcite_plan(thread, serialized.data(), static_cast<long long>(serialized.size()), &responseData, &responseLength) != 0)
    {
        throw UnsupportedQuery("the native Calcite planner failed");
    }
    nes::calcite::PlanResponse response;
    const bool parsed = response.ParseFromArray(responseData, static_cast<int>(responseLength));
    nes_calcite_free(thread, responseData);
    if (not parsed)
    {
        throw UnsupportedQuery("the native Calcite planner returned an unreadable response");
    }
    if (response.unsupported())
    {
        throw UnsupportedQuery("Calcite: {}", response.error());
    }
    if (not response.error().empty())
    {
        throw InvalidQuerySyntax("Calcite rejected the query: {}", response.error());
    }
    return response;
}
#else
/// The planner service (nes-calcite-planner, `java -jar nes-calcite-planner.jar`) at `address`.
nes::calcite::PlanResponse callPlanner(const nes::calcite::PlanRequest& request, const std::string& address)
{
    /// Plans of queries joining dozens of tables exceed gRPC's default 4 MB message size.
    grpc::ChannelArguments channelArguments;
    channelArguments.SetMaxReceiveMessageSize(-1);
    const auto stub = nes::calcite::CalcitePlanner::NewStub(
        grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), channelArguments));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    nes::calcite::PlanResponse response;
    const auto status = stub->Plan(&context, request, &response);
    if (status.error_code() == grpc::StatusCode::INVALID_ARGUMENT)
    {
        throw InvalidQuerySyntax("Calcite rejected the query: {}", status.error_message());
    }
    if (status.error_code() == grpc::StatusCode::UNIMPLEMENTED)
    {
        throw UnsupportedQuery("Calcite: {}", status.error_message());
    }
    if (not status.ok())
    {
        throw UnsupportedQuery("Calcite planner at {} failed: {}", address, status.error_message());
    }
    return response;
}
#endif

nes::calcite::FieldType toFieldType(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::INT8:
            return nes::calcite::INT8;
        case DataType::Type::INT16:
            return nes::calcite::INT16;
        case DataType::Type::INT32:
            return nes::calcite::INT32;
        case DataType::Type::INT64:
            return nes::calcite::INT64;
        case DataType::Type::UINT8:
            return nes::calcite::UINT8;
        case DataType::Type::UINT16:
            return nes::calcite::UINT16;
        case DataType::Type::UINT32:
            return nes::calcite::UINT32;
        case DataType::Type::UINT64:
            return nes::calcite::UINT64;
        case DataType::Type::FLOAT32:
            return nes::calcite::FLOAT32;
        case DataType::Type::FLOAT64:
            return nes::calcite::FLOAT64;
        case DataType::Type::BOOLEAN:
            return nes::calcite::BOOLEAN;
        case DataType::Type::CHAR:
            return nes::calcite::CHAR;
        case DataType::Type::VARSIZED:
            return nes::calcite::VARSIZED;
        case DataType::Type::UNDEFINED:
            break;
    }
    throw UnsupportedQuery("source field of undefined type cannot be planned with Calcite");
}
}

std::string plannerAddress()
{
    const char* address = std::getenv("NES_CALCITE_PLANNER"); /// NOLINT(concurrency-mt-unsafe)
    return address != nullptr ? address : "localhost:50061";
}

bool physicalPlans()
{
    const char* value = std::getenv("NES_CALCITE_PHYSICAL"); /// NOLINT(concurrency-mt-unsafe)
    return value != nullptr and std::string_view{value} == "1";
}

bool finiteInputs()
{
    const char* value = std::getenv("NES_CALCITE_FINITE_INPUTS"); /// NOLINT(concurrency-mt-unsafe)
    return value != nullptr and std::string_view{value} == "1";
}

std::pair<std::string, std::string> splitSink(std::string_view statement)
{
    /// The last INTO starts the sink: a name or a sink type with options in parentheses.
    static const std::regex intoSink(
        R"(^([\s\S]*\S)\s+INTO\s+((?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)(?:\s*\([\s\S]*\))?)\s*;?\s*$)", std::regex::icase);
    std::cmatch match;
    if (not std::regex_match(statement.data(), statement.data() + statement.size(), match, intoSink))
    {
        throw InvalidQuerySyntax("expected '<query> INTO <sink>', got: {}", statement);
    }
    return {match[1].str(), match[2].str()};
}

namespace
{
struct Request
{
    nes::calcite::PlanRequest request;
    std::unordered_map<std::string, std::string> catalogNames; /// name in the SQL -> catalog name
    std::optional<std::string> fallbackSource; /// a catalog source the query may be read from
};

Request makeRequest(
    const std::string_view sql,
    const SourceCatalog& catalog,
    const bool finiteInputs,
    const std::unordered_map<std::string, std::string>& sourceNames,
    const bool physical)
{
    Request result;
    result.request.set_sql(std::string{sql});
    result.request.set_finite_inputs(finiteInputs);
    result.request.set_physical(physical);
    for (const auto& source : catalog.getAllLogicalSources())
    {
        const auto catalogName = fmt::format("{}", source.getLogicalSourceName());
        auto name = catalogName;
        if (not sourceNames.empty())
        {
            const auto alias = sourceNames.find(catalogName);
            if (alias == sourceNames.end())
            {
                continue;
            }
            name = alias->second;
        }
        result.catalogNames.emplace(name, catalogName);
        if (not result.fallbackSource)
        {
            result.fallbackSource = catalogName;
        }
        auto* sourceProto = result.request.add_sources();
        sourceProto->set_name(name);
        for (const auto& field : *source.getSchema())
        {
            auto* fieldProto = sourceProto->add_fields();
            fieldProto->set_name(fmt::format("{}", field.getFullyQualifiedName()));
            fieldProto->set_type(toFieldType(field.getDataType().type));
            fieldProto->set_nullable(field.getDataType().nullable);
        }
        if (physical)
        {
            for (const auto& descriptor : catalog.getPhysicalSources(source).value_or(std::unordered_set<SourceDescriptor>{}))
            {
                auto* physicalSource = sourceProto->add_physical_sources();
                physicalSource->set_id(descriptor.getPhysicalSourceId().getRawValue());
                physicalSource->set_host(descriptor.getHost().getRawValue());
                for (const auto& field : descriptor.getSchema())
                {
                    physicalSource->add_field_names(fmt::format("{}", field.getFullyQualifiedName()));
                }
            }
        }
    }
    return result;
}

substrait::Plan substraitPlanOf(const nes::calcite::PlanResponse& response, const std::string_view sql)
{
    substrait::Plan substraitPlan;
    /// Deep join trees nest far beyond protobuf's default recursion limit of 100.
    google::protobuf::io::CodedInputStream input(
        reinterpret_cast<const uint8_t*>(response.substrait_plan().data()), static_cast<int>(response.substrait_plan().size()));
    input.SetRecursionLimit(100'000);
    if (not substraitPlan.ParseFromCodedStream(&input) or not input.ConsumedEntireMessage())
    {
        throw UnsupportedQuery("Calcite planner returned an unreadable Substrait plan");
    }
    NES_DEBUG("Calcite plan for {}:\n{}", sql, response.explain());
    return substraitPlan;
}
}

LogicalPlan plan(
    const std::string_view sql,
    const std::string_view sinkClause,
    const SourceCatalog& catalog,
    const std::string& address,
    const bool finiteInputs,
    const std::unordered_map<std::string, std::string>& sourceNames)
{
    auto [request, catalogNames, fallbackSource] = makeRequest(sql, catalog, finiteInputs, sourceNames, /*physical=*/false);
    const auto substraitPlan = substraitPlanOf(callPlanner(request, address), sql);
    auto plan = toLogicalPlan(substraitPlan, finiteInputs, fallbackSource, std::move(catalogNames));

    /// The sink clause is ours: let our parser build the sink of a dummy query and put it on top of Calcite's plan.
    if (not fallbackSource)
    {
        throw UnsupportedQuery("Calcite queries need at least one logical source");
    }
    const auto sinkPlan
        = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(fmt::format("SELECT * FROM {} INTO {}", *fallbackSource, sinkClause));
    const auto sink = sinkPlan.getRootOperators().at(0);
    /// Unsafe: the plan is not bound yet, so the sink must not infer its schema now (the optimizer does that).
    plan = plan.withRootOperators({sink.withChildrenUnsafe({plan.getRootOperators().at(0)})});
    plan.setSkippedOptimizerRules({SKIPPED_NES_RULES.begin(), SKIPPED_NES_RULES.end()});
    return plan;
}

namespace
{
/// The worker of a part of a placed physical plan: the host of its last operator (the Sink or a NetworkSink).
std::string partHost(const substrait::Rel& root)
{
    const auto& detail = root.has_extension_single() ? root.extension_single().detail()
        : root.has_extension_multi()                 ? root.extension_multi().detail()
                                                     : root.extension_leaf().detail();
    nes::calcite::PhysicalOperator physical;
    if (not detail.UnpackTo(&physical))
    {
        throw UnsupportedQuery("a part of a physical plan ends in an unknown operator {}", detail.type_url());
    }
    return physical.host();
}
}

PlacedPhysicalQuery planPhysical(
    const std::string_view sql,
    const std::string_view sinkClause,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const std::string& address,
    const bool finiteInputs,
    const std::unordered_map<std::string, std::string>& sourceNames)
{
    auto [request, catalogNames, fallbackSource] = makeRequest(sql, sourceCatalog, finiteInputs, sourceNames, /*physical=*/true);
    if (not fallbackSource)
    {
        throw UnsupportedQuery("Calcite queries need at least one logical source");
    }
    /// The sink clause is in our dialect: our parser reads it as the sink of a dummy query.
    const auto sinkPlan
        = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(fmt::format("SELECT * FROM {} INTO {}", *fallbackSource, sinkClause));
    const auto sink = sinkPlan.getRootOperators().at(0);

    /// The planner places the sink on its host: a named sink's, or the anonymous sink's "host" option (as
    /// AnonymousSinkBindingRule, "host" places the sink, it does not configure it).
    std::optional<SinkDescriptor> namedSink;
    std::unordered_map<Identifier, std::string> anonymousConfig;
    Host sinkHost{Host::INVALID};
    if (const auto named = sink.tryGetAs<SinkLogicalOperator>())
    {
        namedSink = sinkCatalog.getSinkDescriptor(named.value()->getSinkName());
        if (not namedSink)
        {
            throw UnknownSinkName("{}", named.value()->getSinkName());
        }
        sinkHost = namedSink->getHost();
    }
    else if (const auto anonymous = sink.tryGetAs<AnonymousSinkLogicalOperator>())
    {
        anonymousConfig = anonymous.value()->getSinkConfig();
        const auto host = anonymousConfig.find(Identifier::parse("host"));
        if (host == anonymousConfig.end())
        {
            throw InvalidConfigParameter("'host'");
        }
        sinkHost = Host(host->second);
        anonymousConfig.erase(host);
    }
    else
    {
        throw UnsupportedQuery("unknown sink clause {}", sinkClause);
    }

    std::unordered_map<Host, std::string> dataAddresses;
    for (const auto& worker : workerCatalog.getAllWorkers())
    {
        auto* proto = request.add_topology();
        proto->set_host(worker.host.getRawValue());
        if (const auto* limited = std::get_if<CapacityKind::Limited>(&worker.maxOperators))
        {
            proto->set_max_operators(limited->value);
        }
        for (const auto& downstream : worker.downstream)
        {
            proto->add_downstream(downstream.getRawValue());
        }
        dataAddresses.emplace(worker.host, worker.dataAddress);
    }
    request.set_sink_host(sinkHost.getRawValue());
    const auto substraitPlan = substraitPlanOf(callPlanner(request, address), sql);

    BindSink bindSink = [namedSink, anonymousConfig, sink, sinkHost, &sinkCatalog](
                            const Schema<UnqualifiedUnboundField, Ordered>& schema) -> SinkDescriptor
    {
        if (namedSink)
        {
            return *namedSink;
        }
        const auto anonymous = sink.tryGetAs<AnonymousSinkLogicalOperator>().value();
        if (auto descriptor
            = sinkCatalog.getAnonymousSink(schema, anonymous->getSinkType(), sinkHost, anonymousConfig, anonymous->getFormatConfig()))
        {
            return std::move(*descriptor);
        }
        throw InvalidConfigParameter("Failed to create anonymous sink descriptor");
    };
    const auto context = std::make_shared<PhysicalPlanContext>(PhysicalPlanContext{
        .sourceCatalog = sourceCatalog, .sinkCatalog = sinkCatalog, .bindSink = std::move(bindSink), .dataAddresses = std::move(dataAddresses)});

    /// Each worker that runs a part of the plan builds the pipelines of its parts.
    PlacedPhysicalQuery placed;
    for (const auto& relation : substraitPlan.relations())
    {
        const auto host = Host(partHost(relation.root().input()));
        placed.try_emplace(
            host,
            PhysicalQuery{
                .queryId = INVALID_QUERY_ID,
                .pipelines = [substraitPlan, context, host](const QueryId queryId, const QueryExecutionConfiguration& configuration)
                { return toPipelines(substraitPlan, *context, host, queryId, configuration); }});
    }
    return placed;
}

}
