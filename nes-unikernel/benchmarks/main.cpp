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

#include "TestSource.hpp"
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <Operators/LogicalOperators/Network/NesPartition.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <UnikernelExecutionPlan.hpp>

NES::Runtime::BufferManagerPtr TheBufferManager = nullptr;
NES::Runtime::WorkerContextPtr TheWorkerContext = nullptr;

int main() {
    NES::Logger::setupLogging(static_cast<NES::LogLevel>(NES_COMPILE_TIME_LOG_LEVEL));
    errno = 0;
    TheBufferManager = std::make_shared<NES::Runtime::BufferManager>();
    TheBufferManager->createFixedSizeBufferPool(128);
    TheWorkerContext = new NES::Runtime::WorkerContext(NES::Unikernel::CTConfiguration::NodeId, TheBufferManager, 1, 1);

    NES::Unikernel::QueryPlan::setup();

    std::jthread([] {
        for (int i = 0; i < 100000; i++) {
            auto buffer = TheBufferManager->getBufferBlocking();
            auto schemaBuffer = NES::Unikernel::SchemaBuffer<NES::Unikernel::SourceConfig35::Schema, 8192>::of(buffer);
            for (int j = 0; j < schemaBuffer.getCapacity(); j++) {
                NES::Unikernel::SourceConfig35::Schema::TupleType t = std::make_tuple(i, i, i, i, i);
                schemaBuffer.writeTuple(t);
                NES::Unikernel::SourceHandle<NES::Unikernel::SourceConfig35>->emit(buffer);
            }
        }
        NES::Unikernel::SourceHandle<NES::Unikernel::SourceConfig35>->eof();
    });

    NES::Unikernel::QueryPlan::wait();
}