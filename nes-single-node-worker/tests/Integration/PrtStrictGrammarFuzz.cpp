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
#include <thread>

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
#include <ErrorHandling.hpp>
#include <Fuzz.pb.h>
#include <SerializableDataType.pb.h>
#include <SerializableSchema.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

NES::Schema toSm(const NES::SerializableSchema& schema)
{
    return NES::SchemaSerializationUtil::deserializeSchema(schema);
}

NES::DataType toDt(const NES::SerializableDataType& type)
{
    return NES::DataTypeSerializationUtil::deserializeDataType(type);
}

NES::LogicalFunction toLv(const NES::FValue& val)
{
    if (val.has_fieldaccess())
    {
        return NES::FieldAccessLogicalFunction(toDt(val.fieldaccess().datatype()), val.fieldaccess().fieldname());
    }

    if (val.has_const_())
    {
        return NES::ConstantValueLogicalFunction(toDt(val.const_().datatype()), val.const_().valasstr());
    }
    throw NES::CannotDeserialize("foo");
}

NES::LogicalFunction toFn(const NES::FPredicate& pred)
{
    auto left = toLv(pred.left());
    auto right = toLv(pred.right());
    const auto& t = pred.type();
    if (t == NES::FPredicate_Type::FPredicate_Type_LT)
    {
        return NES::LessLogicalFunction(left, right);
    }
    if (t == NES::FPredicate_Type::FPredicate_Type_LE)
    {
        return NES::LessEqualsLogicalFunction(left, right);
    }
    if (t == NES::FPredicate_Type::FPredicate_Type_GE)
    {
        return NES::GreaterEqualsLogicalFunction(left, right);
    }
    if (t == NES::FPredicate_Type::FPredicate_Type_GT)
    {
        return NES::GreaterLogicalFunction(left, right);
    }
    throw NES::CannotDeserialize("foo");
}

NES::LogicalOperator toOp(const NES::FOp& op)
{
    if (op.has_source())
    {
        auto src = NES::OperatorSerializationUtil::deserializeSourceDescriptor(op.source());
        auto srcOp = NES::SourceDescriptorLogicalOperator{std::move(src)};
        return srcOp.withOutputOriginIds({NES::OriginId{op.outputoriginid()}});
    }
    if (op.has_unop())
    {
        const auto& unop = op.unop();
        auto child = toOp(unop.child());
        if (unop.has_select())
        {
            const auto& pred = unop.select().predicate();
            return NES::SelectionLogicalOperator(toFn(pred));
        }
    }
    if (op.has_binop())
    {
    }
    throw NES::CannotDeserialize("foo");
}

NES::LogicalPlan toPlan(const NES::FQueryPlan& plan)
{
    const auto& root = plan.rootoperator();

    auto s = NES::SchemaSerializationUtil::deserializeSchema(root.sinkdescriptor().sinkschema());
    auto c = toOp(plan.rootoperator().child());

    auto sink = NES::SinkLogicalOperator("foo");
    sink.sinkDescriptor = NES::OperatorSerializationUtil::deserializeSinkDescriptor(root.sinkdescriptor());
    return NES::LogicalPlan{sink.withChildren({c})};
}

DEFINE_PROTO_FUZZER(const NES::FQueryPlan& sqp)
{
    NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);

    CPPTRACE_TRY
    {
        auto dqp = toPlan(sqp);
        NES::SingleNodeWorker snw{NES::Configuration::SingleNodeWorkerConfiguration{}};
        auto qid = snw.registerQuery(dqp);
        if (!qid)
        {
            throw qid.error();
        }
        snw.startQuery(*qid);
        snw.stopQuery(*qid, NES::QueryTerminationType::Graceful);
        while (true)
        {
            if (snw.getQuerySummary(*qid)->currentStatus <= NES::QueryStatus::Running)
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
