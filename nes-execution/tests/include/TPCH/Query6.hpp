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
#pragma once

#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionMul.hpp>
#include <Execution/Functions/ArithmeticalFunctions/ExecutableFunctionSub.hpp>
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Functions/LogicalFunctions/AndFunction.hpp>
#include <Execution/Functions/LogicalFunctions/GreaterThanFunction.hpp>
#include <Execution/Functions/LogicalFunctions/LessExecutableFunctionEquals.hpp>
#include <Execution/Functions/LogicalFunctions/LessThanFunction.hpp>
#include <Execution/MemoryProvider/ColumnTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Selection.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <TPCH/PipelinePlan.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Runtime::Execution
{
using namespace Functions;
using namespace Operators;
class TPCH_Query6
{
public:
    static PipelinePlan getPipelinePlan(
        std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables, std::shared_ptr<Memory::AbstractBufferProvider> bm)
    {
        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];

        auto scanMemoryProviderPtr = std::make_unique<Runtime::Execution::MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Memory::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr), projections);

        /*
        *   l_shipdate >= date '1994-01-01'
        *   and l_shipdate < date '1995-01-01'
        */
        auto const_1994_01_01 = std::make_shared<ConstantInt32ValueFunction>(19940101);
        auto const_1995_01_01 = std::make_shared<ConstantInt32ValueFunction>(19950101);
        auto readShipdate = std::make_shared<ExecutableFunctionReadField>("l_shipdate");
        auto lessThanFunction1 = std::make_shared<LessExecutableFunctionEquals>(const_1994_01_01, readShipdate);
        auto lessThanFunction2 = std::make_shared<LessThanFunction>(readShipdate, const_1995_01_01);
        auto andFunction = std::make_shared<AndFunction>(lessThanFunction1, lessThanFunction2);

        auto selection1 = std::make_shared<Selection>(andFunction);
        scan->setChild(selection1);

        /// l_discount between 0.06 - 0.01 and 0.06 + 0.01
        auto readDiscount = std::make_shared<ExecutableFunctionReadField>("l_discount");
        auto const_0_05 = std::make_shared<ConstantFloatValueFunction>(0.04);
        auto const_0_07 = std::make_shared<ConstantFloatValueFunction>(0.08);
        auto lessThanFunction3 = std::make_shared<LessThanFunction>(const_0_05, readDiscount);
        auto lessThanFunction4 = std::make_shared<LessThanFunction>(readDiscount, const_0_07);
        auto andFunction2 = std::make_shared<AndFunction>(lessThanFunction3, lessThanFunction4);
        auto andFunction3 = std::make_shared<AndFunction>(andFunction, andFunction2);

        /// l_quantity < 24
        auto const_24 = std::make_shared<ConstantInt32ValueFunction>(24);
        auto readQuantity = std::make_shared<ExecutableFunctionReadField>("l_quantity");
        auto lessThanFunction5 = std::make_shared<LessThanFunction>(readQuantity, const_24);

        auto andFunction4 = std::make_shared<AndFunction>(andFunction3, lessThanFunction5);

        auto selection2 = std::make_shared<Selection>(andFunction4);
        selection1->setChild(selection2);

        /// sum(l_extendedprice * l_discount)
        auto l_extendedprice = std::make_shared<Functions::ExecutableFunctionReadField>("l_extendedprice");
        auto l_discount = std::make_shared<Functions::ExecutableFunctionReadField>("l_discount");
        auto revenue = std::make_shared<Functions::ExecutableFunctionMul>(l_extendedprice, l_discount);
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions
            = {std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, revenue, "revenue")};
        auto aggregation = std::make_shared<Operators::BatchAggregation>(0 /*handler index*/, aggregationFunctions);
        selection2->setChild(aggregation);

        /// create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scan);
        std::vector<OperatorHandlerPtr> aggregationHandler = {std::make_shared<Operators::BatchAggregationHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);

        auto aggScan = std::make_shared<BatchAggregationScan>(0 /*handler index*/, aggregationFunctions);
        /// emit operator
        auto resultSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
        resultSchema->addField("revenue", BasicType::FLOAT32);
        auto resultLayout = Memory::MemoryLayouts::RowLayout::create(resultSchema, bm.getBufferSize());
        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(resultLayout);
        auto emit = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        aggScan->setChild(emit);

        /// create emit pipeline
        auto emitPipeline = std::make_shared<PhysicalOperatorPipeline>();
        emitPipeline->setRootOperator(aggScan);
        auto pipeline2Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(emitPipeline, pipeline2Context);
        return plan;
    }
};

} /// namespace NES::Runtime::Execution
