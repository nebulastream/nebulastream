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

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Operators/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/MemoryLayoutSelectionPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestQuery.hpp>
#include <gtest/gtest.h>

using namespace NES;
using NES::Runtime::TupleBuffer;

namespace NES {

class MemoryLayoutSelectionPhaseTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MemoryLayoutSelectionPhase.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MemoryLayoutSelectionPhase test case.");
    }

    /* Will be called before a  test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup MemoryLayoutSelectionPhase test case.");

        testSchema = Schema::create()
                         ->addField("test$id", BasicType::INT64)
                         ->addField("test$one", BasicType::INT64)
                         ->addField("test$value", BasicType::INT64);

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    SchemaPtr testSchema;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
};

void fillBufferRowLayout(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout, uint64_t numberOfTuples) {

    auto recordIndexFields = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (size_t recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 100 + recordIndex;
        fields02[recordIndex] = 200 + recordIndex;
    }
    buf.setNumberOfTuples(numberOfTuples);
}

void fillBufferColLayout(TupleBuffer& buf, const Runtime::MemoryLayouts::ColumnLayoutPtr& memoryLayout, uint64_t numberOfTuples) {

    auto recordIndexFields = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (size_t recordIndex = 0; recordIndex < numberOfTuples; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 100 + recordIndex;
        fields02[recordIndex] = 200 + recordIndex;
    }
    buf.setNumberOfTuples(numberOfTuples);
}

TEST_F(MemoryLayoutSelectionPhaseTest, setColumnarLayoutMapQuery) {
    const uint64_t numbersOfBufferToProduce = 1000;
    const uint64_t frequency = 1000;

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->setLayoutType(Schema::MemoryLayoutType::ROW_LAYOUT);

    auto query = TestQuery::from(DefaultSourceDescriptor::create(inputSchema, numbersOfBufferToProduce, frequency))
                     .map(Attribute("f3") = Attribute("f1") * 42)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();
    auto phase = Optimizer::MemoryLayoutSelectionPhase::create(
        Optimizer::MemoryLayoutPolicy::FORCE_COLUMN_LAYOUT);
    phase->execute(plan);

    // Check if all operators in the query have an column layout
    for (auto node : PlanIterator(plan)) {
        if (auto op = node->as_if<Operator>()) {
            ASSERT_EQ(op->getOutputSchema()->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
        }
    }
}

TEST_F(MemoryLayoutSelectionPhaseTest, setRowLayoutMapQuery) {
    const uint64_t numbersOfBufferToProduce = 1000;
    const uint64_t frequency = 1000;

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->setLayoutType(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = TestQuery::from(DefaultSourceDescriptor::create(inputSchema, numbersOfBufferToProduce, frequency))
                     .map(Attribute("f3") = Attribute("f1") * 42)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::MemoryLayoutSelectionPhase::create(
        Optimizer::MemoryLayoutPolicy::FORCE_ROW_LAYOUT);
    phase->execute(plan);

    // Check if all operators in the query have an column layout
    for (auto node : PlanIterator(plan)) {
        if (auto op = node->as_if<Operator>()) {
            ASSERT_EQ(op->getOutputSchema()->getLayoutType(), Schema::MemoryLayoutType::ROW_LAYOUT);
        }
    }
}

TEST_F(MemoryLayoutSelectionPhaseTest, setColumnLayoutWithTypeInference) {
    const uint64_t numbersOfBufferToProduce = 1000;
    const uint64_t frequency = 1000;

    auto inputSchema = Schema::create();
    inputSchema->addField("default_logical$f1", BasicType::INT32);

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = TestQuery::from(DefaultSourceDescriptor::create(inputSchema, numbersOfBufferToProduce, frequency))
                     .filter(Attribute("default_logical$f1") < 10)
                     .map(Attribute("default_logical$f1") = Attribute("default_logical$f1") * 42)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto typeInference = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = typeInference->execute(plan);

    auto phase = Optimizer::MemoryLayoutSelectionPhase::create(
        Optimizer::MemoryLayoutPolicy::FORCE_COLUMN_LAYOUT);
    phase->execute(plan);
    plan = typeInference->execute(plan);
    // Check if all operators in the query have an column layout
    for (auto node : PlanIterator(plan)) {
        if (auto op = node->as_if<Operator>()) {
            ASSERT_EQ(op->getOutputSchema()->getLayoutType(), Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
        }
    }
}

}// namespace NES