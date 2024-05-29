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

#include <CreateProbePipelines.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinProbe.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/ReservoirSample/ReservoirSampleProbe.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>

namespace NES::Runtime::Execution {

std::tuple<std::shared_ptr<PhysicalOperatorPipeline>, Operators::CountMinProbeHandlerPtr>
CreateProbePipelines::createCountMinProbePipeline(const uint64_t numberOfBitsInKey,
                                                  const SchemaPtr inputProbeSchema,
                                                  const std::string_view keyFieldName) {

    // For now, we only allow the inputProbeSchema to be a row memory layout
    NES_ASSERT2_FMT(inputProbeSchema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
                    "Only row memory layout is supported for inputProbeSchema");

    // 1. Creating the scan operator
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(inputProbeSchema, bufferManager->getBufferSize());
    auto scanMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

    // 2. Creating the operator handler that also stores the probe result
    auto countMinProbeOperatorHandler = Operators::CountMinProbeHandler::create();
    pipelineExecutionContext = std::make_shared<ProbePipelineExecutionContext>(countMinProbeOperatorHandler);


    // 3. Creating the count min probe operator
    auto countMinProbeOperator = std::make_shared<Operators::CountMinProbe>(0, keyFieldName, std::make_unique<Nautilus::Interface::H3Hash>(numberOfBitsInKey));

    // 4. Building the pipeline and creating an executable version
    auto pipelineProbe = std::make_shared<PhysicalOperatorPipeline>();
    scanOperator->setChild(countMinProbeOperator);
    pipelineProbe->setRootOperator(scanOperator);
    return {pipelineProbe, countMinProbeOperatorHandler};
}

std::tuple<std::shared_ptr<PhysicalOperatorPipeline>, Operators::ReservoirSampleProbeHandlerPtr>
CreateProbePipelines::createReservoirSamplePipeline(const SchemaPtr inputProbeSchema,
                                                    const SchemaPtr sampleSchema,
                                                    const std::string_view fieldNameToCountInput,
                                                    const std::string_view fieldNameToCountSample) {
    // 1. Creating the scan operator
    auto memoryLayoutScan = Runtime::MemoryLayouts::RowLayout::create(inputProbeSchema, bufferManager->getBufferSize());
    auto scanMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutScan);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

    // 2. Creating the operator handler that also stores the probe result
    auto reservoirSampleProbeOperatorHandler = Operators::ReservoirSampleProbeHandler::create();
    pipelineExecutionContext = std::make_shared<ProbePipelineExecutionContext>(reservoirSampleProbeOperatorHandler);


    // 3. Creating the reservoir sample probe operator
    auto memoryLayoutSample = Runtime::MemoryLayouts::RowLayout::create(sampleSchema, bufferManager->getBufferSize());
    auto sampleMemoryProvider = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutSample);
    auto reservoirSampleProbe = std::make_shared<Operators::ReservoirSampleProbe>(std::move(sampleMemoryProvider),
                                                                                  0,
                                                                                  fieldNameToCountInput,
                                                                                  fieldNameToCountSample);

    // 4. Building the pipeline and creating an executable version
    auto pipelineProbe = std::make_shared<PhysicalOperatorPipeline>();
    scanOperator->setChild(reservoirSampleProbe);
    pipelineProbe->setRootOperator(scanOperator);
    return {pipelineProbe, reservoirSampleProbeOperatorHandler};

}

CreateProbePipelines::CreateProbePipelines() : bufferManager(std::make_shared<BufferManager>()) {
    workerContext = std::make_shared<WorkerContext>(WorkerThreadId(0), bufferManager, 2);
}

} // namespace NES::Runtime::Execution