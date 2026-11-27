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

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/ScalarTypes.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/IsNullCheckLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Plugins/RegistryCatalog.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>
#include <Types/NESTypeCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <BaseUnitTest.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <Model.hpp>
#include <ModelRegistry.hpp>
#include <PlanRuleRegistry.hpp>
#include <SinkConfigRegistry.hpp>
#include <SinkConfigSchemaRegistry.hpp>
#include <SinkRegistry.hpp>
#include <SourceConfigRegistry.hpp>
#include <SourceConfigSchemaRegistry.hpp>
#include <SourceRegistry.hpp>

namespace NES
{
namespace
{

class NESTypeSystemTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NESTypeSystemTest.log", LogLevel::LOG_DEBUG);
    }
};

TEST_F(NESTypeSystemTest, BuiltinRegistriesExposeExpectedTypeClassesAndCollisionFreeSqlFunctors)
{
    const auto& catalog = NESTypeCatalog::instance();

    /// Verify all type classes exposed across all domain registries
    for (const auto* tcName :
         {"ScalarDatatype",
          "NumericDatatype",
          "IntegerDatatype",
          "FloatDatatype",
          "Source",
          "SourceConfigSchema",
          "PhysicalSource",
          "SourceConfig",
          "Sink",
          "SinkConfigSchema",
          "PhysicalSink",
          "SinkConfig",
          "InputFormatter",
          "InputFormatterConfig",
          "InputFormatterConfigSchema",
          "ValueDeserializer",
          "OutputFormatter",
          "OutputFormatterConfig",
          "OutputFormatterConfigSchema",
          "ValueSerializer",
          "PhysicalFunction",
          "AggregationPhysicalFunction",
          "LoweringRule",
          "LogicalFunction",
          "ClosedBinaryFunction",
          "AggregationLogicalFunction",
          "ML_MODEL",
          "LogicalOperator",
          "PlanRule"})
    {
        EXPECT_TRUE(catalog.findTypeClass(QualifiedIdentifier::parse(tcName)).has_value()) << "Missing type class: " << tcName;
    }

    /// Flyweight copy-equality on SQL functor schema and individual functors
    const auto schema1 = catalog.getSqlFunctorSchema();
    const auto schema2 = schema1;
    EXPECT_EQ(schema1, schema2);
    const auto i32Functor = catalog.findSqlFunctor(QualifiedIdentifier::parse("INT32"));
    ASSERT_TRUE(i32Functor.has_value());
    const auto i32FunctorCopy = *i32Functor;
    EXPECT_EQ(*i32Functor, i32FunctorCopy);

    /// Default nullary registry functors instantiate equal NESTypes across repeated calls
    const auto planRuleFunctors = catalog.getFunctorsInRegistry(QualifiedIdentifier::parse("PlanRuleRegistry"));
    ASSERT_FALSE(planRuleFunctors.empty());
    const auto ruleT1 = planRuleFunctors.front().instantiate();
    const auto ruleT2 = planRuleFunctors.front().instantiate();
    ASSERT_TRUE(ruleT1.has_value());
    ASSERT_TRUE(ruleT2.has_value());
    EXPECT_EQ(*ruleT1, *ruleT2);

    /// DataTypeProvider::tryProvideDataType preserves non-throwing std::nullopt contract on malformed and functor-only inputs
    EXPECT_FALSE(DataTypeProvider::tryProvideDataType("").has_value());
    EXPECT_FALSE(DataTypeProvider::tryProvideDataType("NOT A VALID IDENTIFIER").has_value());
    EXPECT_FALSE(DataTypeProvider::tryProvideDataType("OPTIONAL").has_value());

    /// Verify renamed collision-free SQL functors (*Source, *Sink, DataTypes + OPTIONAL, Aggregations, Models, TVFs)
    for (const auto* sqlName :
         {"INT32",
          "UINT64",
          "FLOAT32",
          "VARSIZED",
          "OPTIONAL",
          "FileSource",
          "NetworkSource",
          "FileSink",
          "PrintSink",
          "NetworkSink",
          "Add",
          "FROM_BASE64",
          "Sum",
          "Count",
          "OPENVINO_MODEL",
          "MODEL_INFERENCE"})
    {
        EXPECT_TRUE(catalog.findSqlFunctor(QualifiedIdentifier::parse(sqlName)).has_value()) << "Missing SQL functor: " << sqlName;
    }

    /// Each scalar data type has its own C++ NESType struct, and nullability is modeled via OptionalType
    const auto int32Functor = catalog.findSqlFunctor(QualifiedIdentifier::parse("INT32")).value();
    const auto int8Functor = catalog.findSqlFunctor(QualifiedIdentifier::parse("INT8")).value();
    const auto optionalFunctor = catalog.findSqlFunctor(QualifiedIdentifier::parse("OPTIONAL")).value();
    const auto baseInt32 = int32Functor.instantiate().value();
    const auto baseInt8 = int8Functor.instantiate().value();
    const auto optInt32 = optionalFunctor.instantiate(NESTypeParameters::fromTypes({baseInt32})).value();
    EXPECT_NE(baseInt32, optInt32);
    EXPECT_NE(baseInt32, baseInt8);
    ASSERT_NE(baseInt32.tryGetAs<Int32Type>(), nullptr);
    ASSERT_NE(baseInt8.tryGetAs<Int8Type>(), nullptr);
    EXPECT_EQ(baseInt32.tryGetAs<Int8Type>(), nullptr);
    EXPECT_EQ(baseInt32, (DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_EQ(optInt32, (DataType{DataType::Type::INT32, DataType::NULLABLE::IS_NULLABLE}.toNESType()));
    ASSERT_NE(optInt32.tryGetAs<OptionalType>(), nullptr);
    EXPECT_EQ(optInt32.tryGetAs<OptionalType>()->getInnerType(), baseInt32);

    /// Concrete scalar types construct strongly-typed C++ payloads (`int32_t`, `int8_t`, etc.) with range checks
    const auto nullPayload = OptionalType::createNullValue(baseInt32);
    const auto fortyTwoPayload = Int64Type::createValue(42);
    EXPECT_FALSE(baseInt32.construct(NESValueParameters::positional({nullPayload})).has_value());
    const auto constructedInt32 = baseInt32.construct(NESValueParameters::positional({fortyTwoPayload}));
    ASSERT_TRUE(constructedInt32.has_value());
    ASSERT_NE(constructedInt32->tryGetAs<int32_t>(), nullptr);
    EXPECT_EQ(constructedInt32->getAs<int32_t>(), 42);
    EXPECT_FALSE(baseInt8.construct(NESValueParameters::positional({Int64Type::createValue(999)})).has_value());

    const auto optNullVal = optInt32.construct(NESValueParameters::positional({nullPayload}));
    const auto optFortyTwoVal = optInt32.construct(NESValueParameters::positional({fortyTwoPayload}));
    ASSERT_TRUE(optNullVal.has_value());
    ASSERT_TRUE(optFortyTwoVal.has_value());
    EXPECT_NE(optNullVal->tryGetAs<std::monostate>(), nullptr);
    EXPECT_EQ(optFortyTwoVal->getAs<int32_t>(), 42);
    EXPECT_EQ(*optNullVal, *optInt32.construct(NESValueParameters::positional({nullPayload})));
    EXPECT_NE(*optNullVal, *optFortyTwoVal);

    /// OPTIONAL rejects non-ScalarDatatype arguments such as FileSource
    const auto fileSourceType = catalog.findSqlFunctor(QualifiedIdentifier::parse("FileSource"))->instantiate().value();
    EXPECT_FALSE(optionalFunctor.instantiate(NESTypeParameters::fromTypes({fileSourceType})).has_value());
}

TEST_F(NESTypeSystemTest, ClosedBinaryFunctionValidatesBuiltInAndCustomPluginTypes)
{
    const auto closedBinaryParam = NESTypeCatalog::instance().findTypeClass(QualifiedIdentifier::parse("ClosedBinaryFunction"));
    ASSERT_TRUE(closedBinaryParam.has_value());
    ASSERT_TRUE(closedBinaryParam->getExpectedArguments().has_value());

    const auto addFunctor = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::parse("Add"));
    ASSERT_TRUE(addFunctor.has_value());
    EXPECT_EQ(AddLogicalFunction::functor(), addFunctor->getUnderlyingFunctor());

    /// Passing a string or type instead of a NESFunctor is rejected
    EXPECT_FALSE(
        closedBinaryParam->instantiate(NESTypeClassParameters{NESTypeParameters::fromValues({VarSizedType::createValue("Add")})})
            .has_value());
    EXPECT_FALSE(
        closedBinaryParam
            ->instantiate(
                NESTypeClassParameters{NESTypeParameters::fromTypes({DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()})})
            .has_value());

    const auto addClosedExp
        = closedBinaryParam->instantiate(NESTypeClassParameters{NESTypeParameters::fromFunctors({addFunctor->getUnderlyingFunctor()})});
    ASSERT_TRUE(addClosedExp.has_value());
    const auto& addClosed = *addClosedExp;

    /// Named instantiation with FUNCTION := Add produces an equal NESTypeClass
    const auto addClosedNamedExp = closedBinaryParam->instantiate(NESTypeClassParameters{NESTypeParameters::named(
        Schema<NamedNESTypeParameter, Ordered>{
            NamedNESTypeParameter{QualifiedIdentifier::parse("FUNCTION"), addFunctor->getUnderlyingFunctor()}})});
    ASSERT_TRUE(addClosedNamedExp.has_value());
    EXPECT_EQ(*addClosedNamedExp, addClosed);

    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::INT64, DataType::NULLABLE::IS_NULLABLE}.toNESType()));
    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_FALSE(addClosed.validate(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_FALSE(addClosed.validate(DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));

    /// Sum functor instantiation over INT32 succeeds, over VARSIZED fails cleanly via std::expected
    const auto sumFunctor = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::parse("Sum"));
    ASSERT_TRUE(sumFunctor.has_value());
    EXPECT_EQ(SumAggregationLogicalFunction::functor(), sumFunctor->getUnderlyingFunctor());
    EXPECT_TRUE(
        sumFunctor->instantiate(NESTypeParameters::fromTypes({DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}.toNESType()}))
            .has_value());
    EXPECT_FALSE(
        sumFunctor
            ->instantiate(NESTypeParameters::fromTypes({DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}.toNESType()}))
            .has_value());

    /// Custom scalar type `Vec2f` and custom binary function functor `VecAdd(Vec2f, Vec2f) -> Vec2f`
    /// passed directly as a higher-kinded NESFunctor without needing catalog string lookup
    struct Vec2fNESType
    {
        bool operator==(const Vec2fNESType&) const = default;
    };
    const auto vec2fType = NESType{Vec2fNESType{}}.withTypeTrait(ScalarTypeTrait{});
    EXPECT_TRUE(DataTypeRegistry::scalarDataTypeClass().validate(vec2fType));
    EXPECT_FALSE(vec2fType.hasTypeTrait<LegacyDataTypeTrait>());

    struct VecAddFunctor
    {
        NESType targetType;
        bool operator==(const VecAddFunctor&) const = default;

        [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params) const
        {
            if (params.size() == 2 && params.getTypeAt(0) == targetType && params.getTypeAt(1) == targetType)
            {
                return NESType{Vec2fNESType{}}.withTypeTrait(LogicalFunctionTypeTrait{.resultScalarType = targetType});
            }
            return std::unexpected{TypeInferenceException("VecAdd only defined on (Vec2f, Vec2f)")};
        }
    };
    const NESFunctor vecAddFunctor{VecAddFunctor{.targetType = vec2fType}};

    const auto vecAddClosedExp = closedBinaryParam->instantiate(NESTypeClassParameters{NESTypeParameters::fromFunctors({vecAddFunctor})});
    ASSERT_TRUE(vecAddClosedExp.has_value());
    EXPECT_TRUE(vecAddClosedExp->validate(vec2fType));
    EXPECT_FALSE(vecAddClosedExp->validate(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));

    /// ConstantValueLogicalFunction wraps built-in and custom ScalarDatatype NESValues directly and rejects non-scalars
    const auto vec2fVal = NESValue::create(vec2fType, Vec2fNESType{});
    const auto customConstExp = ConstantValueLogicalFunction::tryCreate(vec2fVal);
    ASSERT_TRUE(customConstExp.has_value());
    EXPECT_EQ(customConstExp->getValue(), vec2fVal);

    const auto nonScalarOpVal = makeLogicalOperatorValue(LogicalOperator{SourceNameLogicalOperator::create(Identifier::parse("S"))});
    EXPECT_FALSE(ConstantValueLogicalFunction::tryCreate(nonScalarOpVal).has_value());

    /// Direct pure type-level expression instantiation (positional & named) without dummy AST/Schema probing
    const auto i32Type = Int32Type::makeNESType();
    const auto i64Type = Int64Type::makeNESType();
    const auto addI32I64Pos = AddLogicalFunction::functor().instantiate(NESTypeParameters::fromTypes({i32Type, i64Type}));
    const auto addI32I64Named = AddLogicalFunction::functor().instantiate(NESTypeParameters::named(
        Schema<NamedNESTypeParameter, Ordered>{
            NamedNESTypeParameter{QualifiedIdentifier::parse("LEFT_TYPE"), i32Type},
            NamedNESTypeParameter{QualifiedIdentifier::parse("RIGHT_TYPE"), i64Type}}));
    const auto addI64I64 = AddLogicalFunction::functor().instantiate(NESTypeParameters::fromTypes({i64Type, i64Type}));
    ASSERT_TRUE(addI32I64Pos.has_value());
    ASSERT_TRUE(addI32I64Named.has_value());
    ASSERT_TRUE(addI64I64.has_value());
    EXPECT_EQ(*addI32I64Pos, *addI32I64Named);
    EXPECT_NE(*addI32I64Pos, *addI64I64);
    EXPECT_EQ(addI32I64Pos->tryGetTypeTrait<LogicalFunctionTypeTrait>()->resultScalarType, i64Type);

    const auto addExprValExp = addI32I64Pos->construct(
        NESValueParameters::positional({Int32Type::createValue(10), Int64Type::createValue(20)}));
    ASSERT_TRUE(addExprValExp.has_value());
    ASSERT_NE(addExprValExp->tryGetAs<LogicalFunction>(), nullptr);
    EXPECT_TRUE(addExprValExp->getAs<LogicalFunction>().tryGetAs<AddLogicalFunction>().has_value());

    const auto absOptI8 = AbsoluteLogicalFunction::functor().instantiate(NESTypeParameters::named(
        Schema<NamedNESTypeParameter, Ordered>{
            NamedNESTypeParameter{QualifiedIdentifier::parse("INPUT_TYPE"), OptionalType::make(Int8Type::makeNESType())}}));
    ASSERT_TRUE(absOptI8.has_value());
    EXPECT_EQ(
        absOptI8->tryGetTypeTrait<LogicalFunctionTypeTrait>()->resultScalarType,
        OptionalType::make(Int16Type::makeNESType()));

    /// Polymorphic built-in functions (`Equals`, `IsNull`, `Count`) work over custom non-DataType `Vec2f` directly
    const auto optVec2f = OptionalType::make(vec2fType);
    const auto eqVec = EqualsLogicalFunction::functor().instantiate(NESTypeParameters::fromTypes({vec2fType, optVec2f}));
    ASSERT_TRUE(eqVec.has_value());
    EXPECT_EQ(
        eqVec->tryGetTypeTrait<LogicalFunctionTypeTrait>()->resultScalarType,
        OptionalType::make(BooleanType::makeNESType()));

    const auto isNullVec = IsNullCheckLogicalFunction::functor().instantiate(NESTypeParameters::fromTypes({optVec2f}));
    ASSERT_TRUE(isNullVec.has_value());
    EXPECT_EQ(isNullVec->tryGetTypeTrait<LogicalFunctionTypeTrait>()->resultScalarType, BooleanType::makeNESType());

    const auto countVec = CountAggregationLogicalFunction::functor().instantiate(NESTypeParameters::fromTypes({vec2fType}));
    ASSERT_TRUE(countVec.has_value());
    EXPECT_EQ(countVec->tryGetTypeTrait<AggregationLogicalFunctionTypeTrait>()->resultScalarType, UInt64Type::makeNESType());
}

TEST_F(NESTypeSystemTest, PolymorphicModelInferenceAcceptsAlternativeMockIreeBackend)
{
    struct MockIreeModelPayload
    {
        std::string modelPath;
        ModelSchema schema;
        bool operator==(const MockIreeModelPayload&) const = default;
    };

    struct MockIreeModelNESType
    {
        using ValueType = MockIreeModelPayload;
        bool operator==(const MockIreeModelNESType&) const = default;
    };

    struct MockIreeModelTypeTrait
    {
        [[nodiscard]] static ModelSchema getSchema(const NESValue& val) { return val.getAs<MockIreeModelPayload>().schema; }

        [[nodiscard]] static std::expected<LogicalOperator, Exception>
        createInferenceOperator(const NESValue&, LogicalOperator child)
        {
            auto proj = ProjectionLogicalOperator::create({}, ProjectionLogicalOperator::Asterisk{true});
            return LogicalOperator{proj.withChildrenUnsafe({std::move(child)})};
        }
    };

    /// Define a second ML model backend implementing MLModelTypeTrait without RegisteredModel / OpenVINO
    const auto mockIreeModelType
        = NESType{MockIreeModelNESType{}}.withTypeTrait(MLModelTypeTrait{MockIreeModelTypeTrait{}});

    const ModelSchema mockSchema{
        .inputs = Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("pixels"), DataType::Type::VARSIZED}},
        .outputs = Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("score"), DataType::Type::FLOAT32}}};

    const auto mockModelVal = NESValue::create(mockIreeModelType, MockIreeModelPayload{.modelPath = "/tmp/iree.vmfb", .schema = mockSchema});
    EXPECT_TRUE(ModelRegistry::mlModelTypeClass().validate(mockModelVal.getType()));

    const auto modelInferenceFunctor = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::parse("MODEL_INFERENCE"));
    ASSERT_TRUE(modelInferenceFunctor.has_value());

    const auto sourceOp = LogicalOperator{SourceNameLogicalOperator::create(Identifier::parse("input_stream"))};
    const auto streamVal = makeLogicalOperatorValue(sourceOp);

    /// Both type-level instantiation and value-level construction succeed polymorphically!
    const auto instantiatedTypeExp
        = modelInferenceFunctor->instantiate(NESTypeParameters::fromTypes({mockModelVal.getType(), streamVal.getType()}));
    ASSERT_TRUE(instantiatedTypeExp.has_value());

    const auto constructedOpValExp = instantiatedTypeExp->construct(NESValueParameters::positional({mockModelVal, streamVal}));
    ASSERT_TRUE(constructedOpValExp.has_value());
    EXPECT_TRUE(LogicalOperatorRegistry::logicalOperatorTypeClass().validate(constructedOpValExp->getType()));
    ASSERT_NE(constructedOpValExp->tryGetAs<LogicalOperator>(), nullptr);
    EXPECT_TRUE(constructedOpValExp->getAs<LogicalOperator>().tryGetAs<ProjectionLogicalOperator>().has_value());

    /// Passing a non-model (e.g. an integer) to MODEL_INFERENCE fails the ML_MODEL type class constraint
    const auto badIntVal = Int64Type::createValue(123);
    const auto rejectedTypeExp
        = modelInferenceFunctor->instantiate(NESTypeParameters::fromTypes({badIntVal.getType(), streamVal.getType()}));
    EXPECT_FALSE(rejectedTypeExp.has_value());
}

}
}
