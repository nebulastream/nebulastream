/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

#include <map>
#include <vector>

#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Util/Logger.hpp>
#include <API/Windowing.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <State/StateManager.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

namespace NES {
class JoinHandlerTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("JoinHandlerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup JoinHandlerTest test class.");
    }

    void TearDown() { NES_INFO("Tear down JoinHandlerTest test class."); }

    const uint64_t buffers_managed = 10;
    const uint64_t buffer_size = 32 * 1024;
};

TEST_F(JoinHandlerTest, testJoinHandlerSlicing) {
    auto store = std::make_unique<Windowing::WindowedJoinSliceListStore<int64_t>>();
    Windowing::WindowTriggerPolicyPtr triggerPolicy =  Windowing::OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
    auto distrType =  Windowing::DistributionCharacteristic::createCompleteWindowType();
    Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
        Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), NES::API::Milliseconds(10)), distrType, triggerPolicy, triggerAction,
        1, 1);

    auto windowManager = std::make_unique<Windowing::WindowManager>(joinDef->getWindowType());
    uint64_t ts = 10;


    windowManager->sliceStream<int64_t, uint64_t>(ts, store.get(), 0UL);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    {
        std::unique_lock lock(store->mutex());
        auto& aggregates = store->getAppendList();
        ASSERT_LT(sliceIndex, aggregates.size());
        aggregates[sliceIndex].emplace_back(10);
    }

    sliceIndex = store->getSliceIndexByTs(ts);
    {
        std::unique_lock lock(store->mutex());
        auto& aggregates = store->getAppendList();
        ASSERT_LT(sliceIndex, aggregates.size());
        aggregates[sliceIndex].emplace_back(11);
    }

    std::unique_lock lock(store->mutex());
    ASSERT_LT(sliceIndex, store->getAppendList().size());
    ASSERT_EQ(store->getAppendList()[sliceIndex].size(), 2);
    ASSERT_EQ(store->getAppendList()[sliceIndex][0], 10);
    ASSERT_EQ(store->getAppendList()[sliceIndex][1], 11);
}
}
