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

Schema toSm(const SerializableSchema& schema)
{
    return SchemaSerializationUtil::deserializeSchema(schema);
}

DataType toDt(const SerializableDataType& type)
{
    return DataTypeSerializationUtil::deserializeDataType(type);
}

LogicalFunction toLv(const FValue& val)
{
    if (val.has_fieldaccess())
    {
        return FieldAccessLogicalFunction(toDt(val.fieldaccess().datatype()), val.fieldaccess().fieldname());
    }

    if (val.has_const_())
    {
        return ConstantValueLogicalFunction(toDt(val.const_().datatype()), val.const_().valasstr());
    }
    throw CannotDeserialize("foo");
}

LogicalFunction toFn(const FPredicate& pred)
{
    auto left = toLv(pred.left());
    auto right = toLv(pred.right());
    const auto& type = pred.type();
    if (type == FPredicate_Type::FPredicate_Type_LT)
    {
        return LessLogicalFunction(left, right);
    }
    if (type == FPredicate_Type::FPredicate_Type_LE)
    {
        return LessEqualsLogicalFunction(left, right);
    }
    if (type == FPredicate_Type::FPredicate_Type_GE)
    {
        return GreaterEqualsLogicalFunction(left, right);
    }
    if (type == FPredicate_Type::FPredicate_Type_GT)
    {
        return GreaterLogicalFunction(left, right);
    }
    throw CannotDeserialize("foo");
}

LogicalOperator toOp(const FOp& op)
{
    if (op.has_source())
    {
        auto src = OperatorSerializationUtil::deserializeSourceDescriptor(op.source());
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
            return SelectionLogicalOperator(toFn(pred));
        }
    }
    if (op.has_binop())
    {
    }
    throw CannotDeserialize("foo");
}

LogicalPlan toPlan(const FQueryPlan& plan)
{
    const auto& root = plan.rootoperator();

    auto s = SchemaSerializationUtil::deserializeSchema(root.sinkdescriptor().sinkschema());
    auto c = toOp(plan.rootoperator().child());

    auto sink = SinkLogicalOperator("foo");
    sink.sinkDescriptor = OperatorSerializationUtil::deserializeSinkDescriptor(root.sinkdescriptor());
    return LogicalPlan{sink.withChildren({c})};
}

DEFINE_PROTO_FUZZER(const FQueryPlan& sqp)
{
    Logger::setupLogging("client.log", LogLevel::LOG_ERROR);

    CPPTRACE_TRY
    {
        auto dqp = toPlan(sqp);
        SingleNodeWorker snw{Configuration::SingleNodeWorkerConfiguration{}};
        auto qid = snw.registerQuery(dqp);
        if (!qid)
        {
            throw qid.error();
        }
        snw.startQuery(*qid);
        snw.stopQuery(*qid, QueryTerminationType::Graceful);
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
