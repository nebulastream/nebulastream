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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <Execution/Operators/MEOS/Meos.hpp>
using namespace std;

namespace NES::Runtime::Execution {

class MeosCreationTest : public ::testing::Test {
  protected:
    Meos* meos;

    MeosCreationTest() : meos(nullptr) {}

        void SetUp() override {
            meos = new Meos();
            
    
        }

        void TearDown() override {
            delete meos;
            meos = nullptr;
        }
};

TEST_F(MeosCreationTest, InitializationTest) {
    ASSERT_NE(meos, nullptr);
}

} // namespace NES::Runtime::Execution