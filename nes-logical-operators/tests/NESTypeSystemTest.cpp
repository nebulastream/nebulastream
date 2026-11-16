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
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
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

    /// Verify core type classes across domain registries
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
          "LogicalFunction",
          "ClosedBinaryFunction",
          "AggregationLogicalFunction",
          "ML_MODEL",
          "LogicalOperator",
          "PlanRule"})
    {
        EXPECT_TRUE(catalog.findTypeClass(QualifiedIdentifier::parse(tcName)).has_value()) << "Missing type class: " << tcName;
    }

    /// Verify renamed collision-free SQL functors (*Source, *Sink, DataTypes, Aggregations, Models, TVFs)
    for (const auto* sqlName :
         {"INT32",
          "UINT64",
          "FLOAT32",
          "VARSIZED",
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
}

TEST_F(NESTypeSystemTest, ClosedBinaryFunctionValidatesBuiltInAndCustomPluginTypes)
{
    const auto closedBinaryParam = NESTypeCatalog::instance().findTypeClass(QualifiedIdentifier::parse("ClosedBinaryFunction"));
    ASSERT_TRUE(closedBinaryParam.has_value());

    const auto addNameVal = NESValue::create(
        DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}.toNESType(),
        ScalarLiteralValue{std::string{"Add"}});
    const auto addClosedExp = closedBinaryParam->instantiate(NESTypeClassParameters{NESTypeParameters::fromValues({addNameVal})});
    ASSERT_TRUE(addClosedExp.has_value());
    const auto& addClosed = *addClosedExp;

    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_TRUE(addClosed.validate(DataType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_FALSE(addClosed.validate(DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
    EXPECT_FALSE(addClosed.validate(DataType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));

    /// Sum functor instantiation over INT32 succeeds, over VARSIZED fails cleanly via std::expected
    const auto sumFunctor = NESTypeCatalog::instance().findSqlFunctor(QualifiedIdentifier::parse("Sum"));
    ASSERT_TRUE(sumFunctor.has_value());
    EXPECT_TRUE(
        sumFunctor->instantiate(NESTypeParameters::fromTypes({DataType{DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE}.toNESType()}))
            .has_value());
    EXPECT_FALSE(
        sumFunctor
            ->instantiate(NESTypeParameters::fromTypes({DataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE}.toNESType()}))
            .has_value());

    /// Dynamically register a custom scalar type `Vec2f` and a custom binary function `VecAdd(Vec2f, Vec2f) -> Vec2f`
    struct Vec2fTag
    {
        bool operator==(const Vec2fTag&) const = default;
    };
    const auto vec2fType = NESType::createUnconstructible<Vec2fTag>(Vec2fTag{})
                               .withTrait(ScalarTypeTrait{.tryGetDataType = [](const ExplicitAny&) { return std::nullopt; }});

    const NESFunctor vecAddFunctor{
        Vec2fTag{},
        [](const Vec2fTag&, const Vec2fTag&) { return true; },
        [vec2fType](const Vec2fTag&, const NESTypeParameters& params) -> std::expected<NESType, Exception>
        {
            if (params.size() == 2 && params.getTypeAt(0) == vec2fType && params.getTypeAt(1) == vec2fType)
            {
                return NESType::createUnconstructible<Vec2fTag>(Vec2fTag{})
                    .withTrait(LogicalFunctionTypeTrait{.resultScalarType = vec2fType});
            }
            return std::unexpected{TypeInferenceException("VecAdd only defined on (Vec2f, Vec2f)")};
        }};

    ASSERT_TRUE(NESTypeCatalog::instance().registerFunctor(
        QualifiedIdentifier::parse("LogicalFunctionRegistry"),
        NamedNESFunctor{QualifiedIdentifier::parse("VecAdd_TestOnly"), vecAddFunctor},
        /*exposeToSqlScope=*/true));

    const auto vecAddClosed = LogicalFunctionRegistry::closedBinaryFunctionTypeClass(QualifiedIdentifier::parse("VecAdd_TestOnly"));
    EXPECT_TRUE(vecAddClosed.validate(vec2fType));
    EXPECT_FALSE(vecAddClosed.validate(DataType{DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE}.toNESType()));
}

TEST_F(NESTypeSystemTest, PolymorphicModelInferenceAcceptsAlternativeMockIreeBackend)
{
    struct MockIreeModelPayload
    {
        std::string modelPath;
        ModelSchema schema;
        bool operator==(const MockIreeModelPayload&) const = default;
    };

    /// Define a second ML model backend implementing MLModelTrait without RegisteredModel / OpenVINO
    const auto mockIreeModelType
        = NESType::createUnconstructible<std::string, MockIreeModelPayload>(std::string{"MOCK_IREE_BACKEND"})
              .withTrait(MLModelTrait{
                  .getSchema = [](const NESValue& val) { return val.getAs<MockIreeModelPayload>().schema; },
                  .createInferenceOperator =
                      [](const NESValue&, LogicalOperator child) -> std::expected<LogicalOperator, Exception>
                  {
                      auto proj = ProjectionLogicalOperator::create({}, ProjectionLogicalOperator::Asterisk{true});
                      return LogicalOperator{proj.withChildrenUnsafe({std::move(child)})};
                  }});

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
    const auto badIntVal = DataType::makeIntegerValue(123);
    const auto rejectedTypeExp
        = modelInferenceFunctor->instantiate(NESTypeParameters::fromTypes({badIntVal.getType(), streamVal.getType()}));
    EXPECT_FALSE(rejectedTypeExp.has_value());
}

}
}
