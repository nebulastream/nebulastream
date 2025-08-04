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

#include <chrono>
#include <memory>
#include <optional>
#include <thread>
#include <utility>

#include <libprotobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <from_current.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/OperatorSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <Fuzz.pb.h>
#include <SerializableDataType.pb.h>
#include <SerializableOperator.pb.h>
#include <SerializableSchema.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

namespace
{

DataType toDt(const SerializableDataType& type)
{
    return DataTypeSerializationUtil::deserializeDataType(type);
}

std::optional<LogicalFunction> toLv(const FValue& val)
{
    if (val.has_fieldaccess())
    {
        return FieldAccessLogicalFunction(toDt(val.fieldaccess().datatype()), val.fieldaccess().fieldname());
    }

    if (val.has_const_())
    {
        return ConstantValueLogicalFunction(toDt(val.const_().datatype()), val.const_().valasstr());
    }
    return {};
}

std::optional<LogicalFunction> toFn(const FPredicate& pred)
{
    auto left = toLv(pred.left());
    if (!left)
    {
        return {};
    }
    auto right = toLv(pred.right());
    if (!right)
    {
        return {};
    }
    const auto& type = pred.type();
    if (type == FPredicate_Type::FPredicate_Type_LT)
    {
        return LessLogicalFunction(*left, *right);
    }
    if (type == FPredicate_Type::FPredicate_Type_LE)
    {
        return LessEqualsLogicalFunction(*left, *right);
    }
    if (type == FPredicate_Type::FPredicate_Type_GE)
    {
        return GreaterEqualsLogicalFunction(*left, *right);
    }
    if (type == FPredicate_Type::FPredicate_Type_GT)
    {
        return GreaterLogicalFunction(*left, *right);
    }
    return {};
}

SourceDescriptor toSd(const SerializableSourceDescriptor& sourceDescriptor)
{
    auto schema = SchemaSerializationUtil::deserializeSchema(sourceDescriptor.sourceschema());
    if (schema.getNumberOfFields() == 0)
    {
        schema.addField("dummy", DataType::Type::BOOLEAN);
    }
    const LogicalSource logicalSource{sourceDescriptor.logicalsourcename(), schema};

    /// TODO #815 the serializer would also a catalog to register/create source descriptors/logical sources
    const auto physicalSourceId = PhysicalSourceId{sourceDescriptor.physicalsourceid()};
    const auto* const sourceType = "File";
    const auto workerIdInt = sourceDescriptor.workerid();
    const auto workerId = WorkerId{workerIdInt};
    const auto buffersInLocalPool = sourceDescriptor.numberofbuffersinlocalpool();

    /// Deserialize the parser config.
    const auto& serializedParserConfig = sourceDescriptor.parserconfig();
    auto deserializedParserConfig = ParserConfig{.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","};

    /// Deserialize SourceDescriptor config. Convert from protobuf variant to SourceDescriptor::ConfigType.
    NES::DescriptorConfig::Config sourceDescriptorConfig{};
    for (const auto& [key, value] : sourceDescriptor.config())
    {
        sourceDescriptorConfig[key] = NES::protoToDescriptorConfigType(value);
    }
    sourceDescriptorConfig["filePath"] = "/dev/random";

    return SourceDescriptor{physicalSourceId, logicalSource, sourceType, (std::move(sourceDescriptorConfig)), deserializedParserConfig};
}

SinkLogicalOperator toSn(const FSinkDscrtr& serializableSinkDescriptor)
{
    /// Declaring variables outside of DescriptorSource for readability/debuggability.
    auto schema = SchemaSerializationUtil::deserializeSchema(serializableSinkDescriptor.sinkschema());
    if (schema.getNumberOfFields() == 0)
    {
        schema.addField("dummy", DataType::Type::BOOLEAN);
    }
    auto addTimestamp = serializableSinkDescriptor.addtimestamp();
    const auto* sinkType = "File";

    /// Deserialize DescriptorSource config. Convert from protobuf variant to DescriptorSource::ConfigType.
    NES::DescriptorConfig::Config sinkDescriptorConfig{};
    for (const auto& [key, desciptor] : serializableSinkDescriptor.config())
    {
        sinkDescriptorConfig[key] = ::NES::protoToDescriptorConfigType(desciptor);
    }

    SinkDescriptor s{"foo", schema, sinkType, sinkDescriptorConfig};
    return SinkLogicalOperator{s};
}

std::optional<LogicalOperator> toOp(const FOp& op)
{
    if (op.has_source())
    {
        auto src = toSd(op.source());
        auto srcOp = SourceDescriptorLogicalOperator{std::move(src)};
        return srcOp.withOutputOriginIds({OriginId{op.outputoriginid()}});
    }
    if (op.has_unop())
    {
        const auto& unop = op.unop();
        auto child = toOp(unop.child());
        if (unop.has_select())
        {
            const auto& pred = unop.select().predicate();
            if (auto fn = toFn(pred); fn.has_value())
            {
                return SelectionLogicalOperator(*toFn(pred));
            }
            return {};
        }
    }
    if (op.has_binop())
    {
    }
    return {};
}

std::optional<LogicalPlan> toPlan(const FQueryPlan& plan)
{
    const auto& root = plan.rootoperator();

    auto schema = SchemaSerializationUtil::deserializeSchema(root.sinkdescriptor().sinkschema());
    auto child = toOp(plan.rootoperator().child());
    if (!child)
    {
        return {};
    }

    auto sink = toSn(root.sinkdescriptor());
    return LogicalPlan{sink.withChildren({*child})};
}

DEFINE_PROTO_FUZZER(const FQueryPlan& sqp)
{
    Logger::setupLogging("client.log", LogLevel::LOG_ERROR);

    CPPTRACE_TRY
    {
        auto dqp = toPlan(sqp);
        if (!dqp)
        {
            return;
        }
        SingleNodeWorker snw{SingleNodeWorkerConfiguration{}};
        auto qid = snw.registerQuery(*dqp);
        if (!qid)
        {
            throw qid.error();
        }
        auto r1 = snw.startQuery(*qid);
        if (!r1.has_value())
        {
            return;
        }
        auto r2 = snw.stopQuery(*qid, QueryTerminationType::Graceful);
        if (!r2.has_value())
        {
            return;
        }
        while (true)
        {
            if (snw.getQuerySummary(*qid)->currentStatus <= QueryStatus::Running)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            else
            {
                return;
            }
        }
    }
    CPPTRACE_CATCH(...)
    {
        return;
    }
}

}

}
