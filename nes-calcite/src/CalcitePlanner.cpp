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
#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Sources/SourceCatalog.hpp>
#include <set>
#include <Configurations/ConfigField.hpp>
#include <InputFormatterConfigSchemaRegistry.hpp>
#include <SourceConfigSchemaRegistry.hpp>
#include <unordered_set>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <google/protobuf/io/coded_stream.h>
#include <grpcpp/grpcpp.h>
#include <substrait/plan.pb.h>
#include <FunctionSignatureRegistry.hpp>
#include <ErrorHandling.hpp>
#include <nes_calcite_planner.grpc.pb.h>

#ifdef NES_CALCITE_NATIVE
    #include <libnes-calcite-planner.h>
#endif

namespace NES::Calcite
{
namespace
{
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
    if (response.placement_failure())
    {
        throw PlacementFailure("Calcite: {}", response.error());
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
    if (status.error_code() == grpc::StatusCode::FAILED_PRECONDITION)
    {
        throw PlacementFailure("Calcite: {}", status.error_message());
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

namespace
{
thread_local std::string requestLabel = "request"; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

void dumpRequest(const nes::calcite::PlanRequest& request)
{
    const char* directory = std::getenv("NES_CALCITE_DUMP_REQUESTS"); /// NOLINT(concurrency-mt-unsafe)
    if (directory == nullptr or *directory == '\0')
    {
        return;
    }
    auto name = requestLabel;
    std::ranges::replace_if(name, [](const char c) { return c == '/' or c == ' '; }, '_');
    std::ofstream file(std::filesystem::path(directory) / (name + ".pb"), std::ios::binary | std::ios::trunc);
    request.SerializeToOstream(&file);
}
}

void setRequestLabel(std::string label)
{
    requestLabel = std::move(label);
}

std::string plannerAddress()
{
    const char* address = std::getenv("NES_CALCITE_PLANNER"); /// NOLINT(concurrency-mt-unsafe)
    return address != nullptr ? address : "localhost:50061";
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
    std::unordered_set<std::string> caseSensitive; /// case-sensitive (quoted) source and field names, as sent to Calcite
};

Request makeRequest(
    const std::string_view sql,
    const SourceCatalog& catalog,
    const bool finiteInputs,
    const std::unordered_map<std::string, std::string>& sourceNames,
    const bool physical,
    const ModelCatalog* models)
{
    Request result;
    /// The registered source types and input formatter options, for anonymous sources (TABLE(FILE(...)) etc.).
    for (const auto& type : SourceConfigSchemaRegistry::instance().getRegisteredNames())
    {
        auto* typeProto = result.request.add_source_types();
        typeProto->set_name(type);
        for (const auto& option : SourceConfigSchemaRegistry::instance().getSchema(type).value_or(Schema<QualifiedErasedConfigField, Ordered>{}))
        {
            typeProto->add_options(std::ranges::prev(option.getFullyQualifiedName().end())->asCanonicalString());
        }
    }
    std::set<std::string> formatOptions;
    for (const auto& type : InputFormatterConfigSchemaRegistry::instance().getRegisteredNames())
    {
        for (const auto& option : InputFormatterConfigSchemaRegistry::instance().getSchema(type).value_or(Schema<QualifiedErasedConfigField, Ordered>{}))
        {
            formatOptions.insert(std::ranges::prev(option.getFullyQualifiedName().end())->asCanonicalString());
        }
    }
    for (const auto& option : formatOptions)
    {
        result.request.add_format_options(option);
    }
    /// Our functions that SQL does not have.
    for (const auto& name : FunctionSignatureRegistry::instance().getRegisteredNames())
    {
        const auto signature = FunctionSignatureRegistry::instance().find(name).value();
        auto* function = result.request.add_functions();
        function->set_name(name);
        for (const auto& operand : signature.operands)
        {
            function->add_operands(operand);
        }
        function->set_result(signature.result);
        function->set_aggregation(signature.aggregation);
    }
    /// The registered models, for MODEL_INFERENCE.
    for (const auto& model : models == nullptr ? std::vector<RegisteredModel>{} : models->getRegisteredModels())
    {
        /// Offered under the name the query uses, as sources are (e.g. systest's unprefixed names).
        auto name = model.getName();
        if (not sourceNames.empty())
        {
            const auto alias = sourceNames.find(name);
            if (alias == sourceNames.end())
            {
                continue;
            }
            name = alias->second;
        }
        result.catalogNames.emplace(name, model.getName());
        auto* modelProto = result.request.add_models();
        modelProto->set_name(name);
        const auto addFields = [](const auto& schema, auto* fields)
        {
            for (const auto& field : schema)
            {
                auto* fieldProto = fields->Add();
                fieldProto->set_name(fmt::format("{}", field.getFullyQualifiedName()));
                fieldProto->set_type(toFieldType(field.getDataType().type));
                fieldProto->set_nullable(field.getDataType().nullable);
            }
        };
        addFields(model.getSchema().inputs, modelProto->mutable_inputs());
        addFields(model.getSchema().outputs, modelProto->mutable_outputs());
    }
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
        if (source.getLogicalSourceName().isCaseSensitive())
        {
            result.caseSensitive.insert(catalogName);
        }
        auto* sourceProto = result.request.add_sources();
        sourceProto->set_name(name);
        for (const auto& field : *source.getSchema())
        {
            if (std::ranges::any_of(field.getFullyQualifiedName(), [](const auto& part) { return part.isCaseSensitive(); }))
            {
                result.caseSensitive.insert(fmt::format("{}", field.getFullyQualifiedName()));
            }
            auto* fieldProto = sourceProto->add_fields();
            fieldProto->set_name(fmt::format("{}", field.getFullyQualifiedName()));
            fieldProto->set_type(toFieldType(field.getDataType().type));
            fieldProto->set_nullable(field.getDataType().nullable);
        }
        /// Also for logical plans, which do not use them: a saved request (NES_CALCITE_DUMP_REQUESTS) can then be planned
        /// down to placed pipelines, too.
        {
            for (const auto& descriptor : catalog.getPhysicalSources(source).value_or(std::unordered_set<SourceDescriptor>{}))
            {
                auto* physicalSource = sourceProto->add_physical_sources();
                physicalSource->set_id(descriptor.getPhysicalSourceId().getRawValue());
                physicalSource->set_host(descriptor.getHost().getRawValue());
                physicalSource->set_format(descriptor.getInputFormatterDescriptor().getInputFormatterType().asCanonicalString());
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

PlannedPhysicalQuery planPhysical(
    const std::string_view sql,
    const std::string_view sinkClause,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const std::string& address,
    const bool finiteInputs,
    const std::unordered_map<std::string, std::string>& sourceNames,
    const ModelCatalog* models,
    const QueryOptimizerConfiguration& optimizer,
    const bool detailedSteps)
{
    auto [request, catalogNames, caseSensitive] = makeRequest(sql, sourceCatalog, finiteInputs, sourceNames, /*physical=*/true, models);
    request.set_nested_loop_joins(optimizer.joinStrategy == StreamJoinStrategy::NESTED_LOOP_JOIN);
    request.set_detailed_steps(detailedSteps);
    request.set_layout_policy(
        optimizer.memoryLayout == MemoryLayoutPolicy::COLUMNAR ? nes::calcite::LayoutPolicy::COLUMNS : nes::calcite::LayoutPolicy::ROWS);
    /// The sink clause is in our dialect, which our parser reads.
    const auto sink = AntlrSQLQueryParser::bindSinkClause(sinkClause);
    const auto* const anonymous = std::get_if<AntlrSQLQueryParser::AnonymousSinkClause>(&sink);

    /// The planner places the sink on its host: a named sink's, or the anonymous sink's "host" option (as
    /// AnonymousSinkBindingRule, "host" places the sink, it does not configure it).
    std::optional<SinkDescriptor> namedSink;
    std::unordered_map<Identifier, std::string> anonymousConfig;
    Host sinkHost{Host::INVALID};
    if (const auto* const named = std::get_if<Identifier>(&sink))
    {
        namedSink = sinkCatalog.getSinkDescriptor(*named);
        if (not namedSink)
        {
            throw UnknownSinkName("{}", *named);
        }
        sinkHost = namedSink->getHost();
    }
    else
    {
        anonymousConfig = anonymous->config;
        const auto host = anonymousConfig.find(Identifier::parse("host"));
        if (host == anonymousConfig.end())
        {
            throw InvalidConfigParameter("'host'");
        }
        sinkHost = Host(host->second);
        anonymousConfig.erase(host);
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
    /// The sink runs on its host, which must be a worker (also without a topology to place on).
    if (not dataAddresses.contains(sinkHost))
    {
        throw PlacementFailure(
            "SINK({}) was placed on non-existing worker '{}'",
            namedSink ? std::get<Identifier>(sink).asCanonicalString() : std::string("anonymous"),
            sinkHost.getRawValue());
    }
    request.set_sink_host(sinkHost.getRawValue());
    /// The sink's format: a named sink's output formatter, an anonymous sink's OUTPUT_FORMAT option (as the sink catalog
    /// reads it), else NATIVE.
    if (namedSink)
    {
        request.set_sink_format(namedSink->getOutputFormatterDescriptor().getOutputFormatterType().asCanonicalString());
    }
    else if (const auto format = anonymousConfig.find(Identifier::parse("OUTPUT_FORMAT")); format != anonymousConfig.end())
    {
        request.set_sink_format(Identifier::parse(format->second).asCanonicalString());
    }
    else
    {
        request.set_sink_format("NATIVE");
    }
    dumpRequest(request);
    const auto response = callPlanner(request, address);
    const auto substraitPlan = substraitPlanOf(response, sql);

    BindSink bindSink = [namedSink, anonymousConfig, sink, sinkHost, &sinkCatalog](
                            const Schema<UnqualifiedUnboundField, Ordered>& schema) -> SinkDescriptor
    {
        if (namedSink)
        {
            return *namedSink;
        }
        /// An anonymous sink's own schema if it has one (e.g. systest's rewrite of a declared sink), else the result's.
        const auto& anonymous = std::get<AntlrSQLQueryParser::AnonymousSinkClause>(sink);
        if (auto descriptor = sinkCatalog.getAnonymousSink(
                anonymous.schema.value_or(schema), anonymous.type, sinkHost, anonymousConfig, anonymous.formatConfig))
        {
            return std::move(*descriptor);
        }
        throw InvalidConfigParameter("Failed to create anonymous sink descriptor");
    };
    const auto context = std::make_shared<PhysicalPlanContext>(PhysicalPlanContext{
        .sourceCatalog = sourceCatalog,
        .sinkCatalog = sinkCatalog,
        .bindSink = std::move(bindSink),
        .dataAddresses = std::move(dataAddresses),
        .models = models,
        .catalogNames = catalogNames});

    /// As our logical sink checks it: the schema of a named sink, or an anonymous sink's own one, has the result's fields, in
    /// any order.
    auto inputSchema = sinkInputSchema(substraitPlan);
    const auto targetSchema = namedSink
        ? std::optional{*std::get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(namedSink->getSchema())}
        : anonymous->schema;
    if (targetSchema)
    {
        const auto& declared = *targetSchema;
        const auto missing = [](const auto& full, const auto& partial)
        {
            std::vector<UnqualifiedUnboundField> fields;
            for (const auto& field : full)
            {
                if (const auto found = partial[field.getFullyQualifiedName()]; not found or found->getDataType() != field.getDataType())
                {
                    fields.push_back(field);
                }
            }
            return fields;
        };
        const auto expectedButNotFound = missing(declared, inputSchema);
        const auto unexpected = missing(inputSchema, declared);
        if (not expectedButNotFound.empty() or not unexpected.empty())
        {
            throw CannotInferSchema(
                "The schema of the sink must be equal to the schema of the input operator. Expected fields {} where not found, and "
                "found unexpected fields {}",
                expectedButNotFound,
                unexpected);
        }
    }
    const auto sinkType = namedSink ? namedSink->getSinkType() : anonymous->type.asCanonicalString();

    /// Each part is a query of its own on its worker, which builds its pipelines.
    PlacedPhysicalQuery placed;
    for (int relation = 0; relation < substraitPlan.relations_size(); ++relation)
    {
        const auto host = Host(partHost(substraitPlan.relations(relation).root().input()));
        placed[host].push_back(PhysicalQuery{.queryId = INVALID_QUERY_ID, .part = resolveWorkerPart(substraitPlan, relation, *context, host)});
    }
    /// The sink writes its own schema, if it has one: that is the result systest and others read.
    std::vector<PlanStep> steps;
    for (const auto& step : response.steps())
    {
        std::vector<PlanStep::Rule> rules;
        for (const auto& rule : step.rules())
        {
            rules.push_back({.rule = rule.rule(), .ours = rule.ours(), .plan = rule.plan(), .dot = rule.dot()});
        }
        steps.push_back({.name = step.name(), .origin = step.origin(), .plan = step.plan(), .dot = step.dot(), .rules = std::move(rules)});
    }
    return PlannedPhysicalQuery{
        .parts = std::move(placed),
        .sinkInputSchema = targetSchema.value_or(std::move(inputSchema)),
        .sinkType = sinkType,
        .explain = response.explain(),
        .steps = std::move(steps)};
}


PlannedPhysicalQuery planStatement(
    const std::string_view statement,
    const SourceCatalog& sourceCatalog,
    const SinkCatalog& sinkCatalog,
    const WorkerCatalog& workerCatalog,
    const ModelCatalog* models,
    const QueryOptimizerConfiguration& optimizer,
    const bool detailedSteps)
{
    const auto [query, sinkClause] = splitSink(statement);
    return planPhysical(
        query, sinkClause, sourceCatalog, sinkCatalog, workerCatalog, plannerAddress(), finiteInputs(), {}, models, optimizer, detailedSteps);
}

}
