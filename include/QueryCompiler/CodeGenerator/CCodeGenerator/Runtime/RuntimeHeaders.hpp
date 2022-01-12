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

#ifndef NES_RUNTIMEHEADERS_HPP
#define NES_RUNTIMEHEADERS_HPP
#include <string>
namespace NES{
namespace QueryCompilation{
[[maybe_unused]] inline static const std::string ARRAY = "#include <Common/ExecutableType/Array.hpp>";
[[maybe_unused]] inline static const std::string CEP_OPERATOR_HANDLER = "#include <QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/CEPOperatorHandler.hpp>";
[[maybe_unused]] inline static const std::string C_STD_INT = "#include <cstdint>";
[[maybe_unused]] inline static const std::string STRING = "#include <string.h>";
[[maybe_unused]] inline static const std::string STATE_VARIABLE = "#include <State/StateVariable.hpp>";
[[maybe_unused]] inline static const std::string LOGICAL_WINDOW_DEFINITION =  "#include <Windowing/LogicalWindowDefinition.hpp>";
[[maybe_unused]] inline static const std::string AGGREGATION_WINDOW_HANDLER =  "#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>";
[[maybe_unused]] inline static const std::string WINDOW_OPERATOR_HANDLER =  "#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>";
[[maybe_unused]] inline static const std::string BASE_EXECUTABLE_JOIN_ACTION =   "#include <Windowing/WindowActions/BaseExecutableJoinAction.hpp>";
[[maybe_unused]] inline static const std::string WINDOW_MANAGER =  "#include <Windowing/Runtime/WindowManager.hpp>";
[[maybe_unused]] inline static const std::string WINDOW_SLICE_STORE =  "#include <Windowing/Runtime/WindowSliceStore.hpp>";
[[maybe_unused]] inline static const std::string JOIN_OPERATOR_HANDLER =  "#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>";
[[maybe_unused]] inline static const std::string EXECUTABLE_ON_TIME_TRIGGER_POLICY = "#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>";
[[maybe_unused]] inline static const std::string EXECUTABLE_ON_WATERMARK_CHANGE_TRIGGER_POLICY =   "#include <Windowing/WindowPolicies/ExecutableOnWatermarkChangeTriggerPolicy.hpp>";
[[maybe_unused]] inline static const std::string JOIN_HANDLER = "#include <Windowing/WindowHandler/JoinHandler.hpp>";
[[maybe_unused]] inline static const std::string EXECUTABLE_NESTED_LOOP_JOIN_TRIGGER_ACTION =  "#include <Windowing/WindowActions/ExecutableNestedLoopJoinTriggerAction.hpp>";
[[maybe_unused]] inline static const std::string WINDOWED_JOIN_SLICE_STORE = "#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>";
[[maybe_unused]] inline static const std::string COUNT_AGGR = "#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>";
[[maybe_unused]] inline static const std::string SUM_AGGR = "#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>";
[[maybe_unused]] inline static const std::string MIN_AGGR = "#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>";
[[maybe_unused]] inline static const std::string MAX_AGGR = "#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>";
[[maybe_unused]] inline static const std::string AVG_AGGR = "#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>";
[[maybe_unused]] inline static const std::string MEDIAN_AGGR = "#include <Windowing/WindowAggregations/ExecutableMedianAggregation.hpp>";
[[maybe_unused]] inline static const std::string EXECUTABLE_SLICE_AGGR_TRIGGER_ACTION = "#include <Windowing/WindowActions/ExecutableSliceAggregationTriggerAction.hpp>";
[[maybe_unused]] inline static const std::string EXECUTABLE_COMPLETE_AGGR_TRIGGER_ACTION = "#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>";
}//namespace QueryCompilation
}//namesapce NES

#endif//NES_RUNTIMEHEADERS_HPP
