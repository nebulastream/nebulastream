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
#ifndef NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY1_HPP_
#define NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY1_HPP_

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/MulExpression.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <Execution/Expressions/ConstantValueExpression.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Expressions/LogicalExpressions/AndExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/GreaterThanExpression.hpp>
#include <Execution/Expressions/LogicalExpressions/LessThanExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationHandler.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchAggregationScan.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregation.hpp>
#include <Execution/Operators/Relational/Aggregation/BatchKeyedAggregationHandler.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TPCH/PipelinePlan.hpp>
#include <TPCH/TPCHTableGenerator.hpp>
#include <Util/TestTupleBuffer.hpp>
namespace NES::Runtime::Execution {
using namespace Expressions;
using namespace Operators;
class TPCH_Query1 {
  public:
    static PipelinePlan getPipelinePlan(std::unordered_map<TPCHTable, std::unique_ptr<NES::Runtime::Table>>& tables,
                                        Runtime::BufferManagerPtr) {
        PipelinePlan plan;
        auto& lineitems = tables[TPCHTable::LineItem];
        auto physicalTypeFactory = DefaultPhysicalTypeFactory();
        PhysicalTypePtr integerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt32());
        PhysicalTypePtr uintegerType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createUInt64());
        PhysicalTypePtr floatType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createFloat());

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(
            std::dynamic_pointer_cast<Runtime::MemoryLayouts::ColumnLayout>(lineitems->getLayout()));
        std::vector<std::string> projections = {"l_shipdate", "l_discount", "l_quantity", "l_extendedprice"};
        auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        /*
     * l_shipdate <= date '1998-12-01' - interval '90' day
     *
     *  1998-09-02
     */
        auto const_1998_09_02 = std::make_shared<ConstantInt32ValueExpression>(19980831);
        auto readShipdate = std::make_shared<ReadFieldExpression>("l_shipdate");
        auto lessThanExpression1 = std::make_shared<LessThanExpression>(readShipdate, const_1998_09_02);
        auto selection = std::make_shared<Selection>(lessThanExpression1);
        scan->setChild(selection);

        /*
     *
     * group by
        l_returnflag,
        l_linestatus
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order

        rewrite to
        sum(l_quantity) as sum_qty
        sum(l_extendedprice)
        disc_price = l_extendedprice * (1 - l_discount)
        sum(disc_price)
        sum(disc_price * (one + l_tax[i]))
        count(*)
     */
        auto l_returnflagField = std::make_shared<ReadFieldExpression>("l_returnflag");
        auto l_linestatusFiled = std::make_shared<ReadFieldExpression>("l_linestatus");

        //  sum(l_quantity) as sum_qty,
        auto l_quantityField = std::make_shared<ReadFieldExpression>("l_quantity");
        auto sumAggFunction1 =
            std::make_shared<Aggregation::SumAggregationFunction>(integerType, integerType, l_quantityField, "sum_qty");

        // sum(l_extendedprice) as sum_base_price,
        auto l_extendedpriceField = std::make_shared<ReadFieldExpression>("l_extendedprice");
        auto sumAggFunction2 =
            std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType, l_extendedpriceField, "sum_base_price");

        // disc_price = l_extendedprice * (1 - l_discount)
        auto l_discountField = std::make_shared<ReadFieldExpression>("l_discount");
        auto oneConst = std::make_shared<ConstantFloatValueExpression>(1.0f);
        auto subExpression = std::make_shared<SubExpression>(oneConst, l_discountField);
        auto mulExpression = std::make_shared<MulExpression>(l_extendedpriceField, subExpression);
        auto disc_priceExpression = std::make_shared<WriteFieldExpression>("disc_price", mulExpression);
        auto map = std::make_shared<Map>(disc_priceExpression);
        selection->setChild(map);

        //  sum(disc_price)
        auto disc_price = std::make_shared<ReadFieldExpression>("disc_price");
        auto sumAggFunction3 =
            std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType, disc_price, "sum_disc_price");

        //  sum(disc_price * (one + l_tax[i]))
        auto l_taxField = std::make_shared<ReadFieldExpression>("l_tax");
        auto addExpression = std::make_shared<AddExpression>(oneConst, l_taxField);
        auto mulExpression2 = std::make_shared<AddExpression>(disc_price, addExpression);
        auto sumAggFunction4 =
            std::make_shared<Aggregation::SumAggregationFunction>(floatType, floatType, mulExpression2, "sum_charge");

        //   count(*)
        auto countAggFunction5 = std::make_shared<Aggregation::CountAggregationFunction>(uintegerType,
                                                                                         uintegerType,
                                                                                         Expressions::ExpressionPtr(),
                                                                                         "count_order");

        std::vector<Expressions::ExpressionPtr> keyFields = {l_returnflagField, l_linestatusFiled};
        std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions = {sumAggFunction1,
                                                                                               sumAggFunction2,
                                                                                               sumAggFunction3,
                                                                                               sumAggFunction4,
                                                                                               countAggFunction5};

        PhysicalTypePtr smallType = physicalTypeFactory.getPhysicalType(DataTypeFactory::createInt8());
        std::vector<PhysicalTypePtr> types = {smallType, smallType};
        auto aggregation =
            std::make_shared<Operators::BatchKeyedAggregation>(0 /*handler index*/,
                                                               keyFields,
                                                               types,
                                                               aggregationFunctions,
                                                               std::make_unique<Nautilus::Interface::MurMur3HashFunction>());

        map->setChild(aggregation);

        // create aggregation pipeline
        auto aggregationPipeline = std::make_shared<PhysicalOperatorPipeline>();
        aggregationPipeline->setRootOperator(scan);
        std::vector<OperatorHandlerPtr> aggregationHandler = {std::make_shared<Operators::BatchKeyedAggregationHandler>()};
        auto pipeline1Context = std::make_shared<MockedPipelineExecutionContext>(aggregationHandler);
        plan.appendPipeline(aggregationPipeline, pipeline1Context);
        return plan;
    }
};

}// namespace NES::Runtime::Execution
#endif // NES_EXECUTION_TESTS_INCLUDE_TPCH_QUERY1_HPP_
