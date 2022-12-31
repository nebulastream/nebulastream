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

//#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Benchmark {
    class E2EBenchmarkConfigPerRunTest : public testing::Test {
      public:
        /* Will be called before any test in this class are executed. */
        static void SetUpTestCase() {
            NES::Logger::setupLogging("E2EBenchmarkConfigPerRunTest.log", NES::LogLevel::LOG_DEBUG);
            NES_INFO("Setup E2EBenchmarkConfigPerRunTest test class.");
        }

        /* Will be called before a test is executed. */
        void SetUp() override { NES_INFO("Setup E2EBenchmarkConfigPerRunTest test case."); }

        /* Will be called before a test is executed. */
        void TearDown() override { NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test case."); }

        /* Will be called after all tests in this class are finished. */
        static void TearDownTestCase() { NES_INFO("Tear down E2EBenchmarkConfigPerRunTest test class."); }
    };

    TEST_F(E2EBenchmarkConfigPerRunTest, toStringTest) {
        std::stringstream oss;

        // set default E2EBenchmarkConfigPerRun
        // call function toString()

        oss << "- numWorkerOfThreads: " << numberOfWorkerThreads->getValueAsString() << std::endl
            << "- bufferSizeInBytes: " << bufferSizeInBytes->getValueAsString() << std::endl
            << "- numberOfQueriesToDeploy: " << numberOfQueriesToDeploy->getValueAsString() << std::endl
            << "- numberOfSources: " << getStrLogicalSrcToNumberOfPhysicalSrc() << std::endl
            << "- numberOfBuffersInGlobalBufferManager: " << numberOfBuffersInGlobalBufferManager->getValueAsString() << std::endl
            << "- numberOfBuffersInSourceLocalBufferPool: " << numberOfBuffersInSourceLocalBufferPool->getValueAsString()
            << std::endl;
        auto expectedString = oss.str();

        // ASSERT_EQ(stringDefault, expectedString);
    }//TEST_F(E2EBenchmarkConfigPerRunTest, toStringTest)

    TEST_F(E2EBenchmarkConfigPerRunTest, generateAllConfigsPerRunTest) {
        std::vector<E2EBenchmarkConfigPerRun> expectedAllConfigPerRuns;
        E2EBenchmarkConfigPerRun configPerRun;

        // set default E2EBenchmarkConfigPerRun
        // call function generateAllConfigsPerRun()

        /* Getting all parameters per experiment run in vectors */
        auto numWorkerOfThreads = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfWorkerThreads"].As<std::string>(),
                                                                      configPerRun.numberOfWorkerThreads->getDefaultValue());

        auto bufferSizeInBytes = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["bufferSizeInBytes"].As<std::string>(),
                                                                     configPerRun.bufferSizeInBytes->getDefaultValue());
        auto numberOfQueriesToDeploy = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfQueriesToDeploy"].As<std::string>(),
                                                                           configPerRun.numberOfQueriesToDeploy->getDefaultValue());

        auto numberOfBuffersInGlobalBufferManager =
            Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInGlobalBufferManager"].As<std::string>(),
                                                configPerRun.numberOfBuffersInGlobalBufferManager->getDefaultValue());

        auto numberOfBuffersInSourceLocalBufferPool =
            Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInSourceLocalBufferPool"].As<std::string>(),
                                                configPerRun.numberOfBuffersInSourceLocalBufferPool->getDefaultValue());


        std::vector<std::map<std::string, uint64_t>> allLogicalSrcToPhysicalSources = {configPerRun.logicalSrcToNoPhysicalSrc};
        if (!yamlConfig["logicalSources"].IsNone()) {
            allLogicalSrcToPhysicalSources = E2EBenchmarkConfigPerRun::generateMapsLogicalSrcPhysicalSources(yamlConfig);
        }

        /* Retrieving the maximum number of experiments to run */
        size_t totalBenchmarkRuns = numWorkerOfThreads.size();
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, bufferSizeInBytes.size());
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfQueriesToDeploy.size());
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInGlobalBufferManager.size());
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInSourceLocalBufferPool.size());
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, allLogicalSrcToPhysicalSources.size());

        /* Padding all vectors to the desired size */
        Util::padVectorToSize<uint32_t>(numWorkerOfThreads, totalBenchmarkRuns, numWorkerOfThreads.back());
        Util::padVectorToSize<uint32_t>(bufferSizeInBytes, totalBenchmarkRuns, bufferSizeInBytes.back());
        Util::padVectorToSize<uint32_t>(numberOfQueriesToDeploy, totalBenchmarkRuns, numberOfQueriesToDeploy.back());
        Util::padVectorToSize<uint32_t>(numberOfBuffersInGlobalBufferManager,
                                        totalBenchmarkRuns,
                                        numberOfBuffersInGlobalBufferManager.back());
        Util::padVectorToSize<uint32_t>(numberOfBuffersInSourceLocalBufferPool,
                                        totalBenchmarkRuns,
                                        numberOfBuffersInSourceLocalBufferPool.back());
        Util::padVectorToSize<std::map<std::string, uint64_t>>(allLogicalSrcToPhysicalSources, totalBenchmarkRuns, allLogicalSrcToPhysicalSources.back());

        expectedAllConfigPerRuns.reserve(totalBenchmarkRuns);
        for (size_t i = 0; i < totalBenchmarkRuns; ++i) {
            E2EBenchmarkConfigPerRun e2EBenchmarkConfigPerRun;
            e2EBenchmarkConfigPerRun.numberOfWorkerThreads->setValue(numWorkerOfThreads[i]);
            e2EBenchmarkConfigPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes[i]);
            e2EBenchmarkConfigPerRun.numberOfQueriesToDeploy->setValue(numberOfQueriesToDeploy[i]);
            e2EBenchmarkConfigPerRun.numberOfBuffersInGlobalBufferManager->setValue(numberOfBuffersInGlobalBufferManager[i]);
            e2EBenchmarkConfigPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(numberOfBuffersInSourceLocalBufferPool[i]);
            e2EBenchmarkConfigPerRun.logicalSrcToNoPhysicalSrc = allLogicalSrcToPhysicalSources[i];

            expectedAllConfigPerRuns.push_back(e2EBenchmarkConfigPerRun);
        }

        // ASSERT_EQ(defaultAllConfigPerRuns.size(), expectedAllConfigPerRuns.size());
        // ASSERT_TRUE(memcmp(defaultAllConfigPerRuns, expectedAllConfigPerRuns, defaultAllConfigPerRuns.size()) == 0);
    }//TEST_F(E2EBenchmarkConfigPerRunTest, generateAllConfigsPerRunTest)

    TEST_F(E2EBenchmarkConfigPerRunTest, getStrLogicalSrcToNumberOfPhysicalSrcTest) {
        std::stringstream expectedString;

        // set default E2EBenchmarkConfigPerRun
        // call function getStrLogicalSrcToNumberOfPhysicalSrc()

        for (auto it = logicalSrcToNoPhysicalSrc.begin(); it != logicalSrcToNoPhysicalSrc.end(); ++it) {
            if (it != logicalSrcToNoPhysicalSrc.begin()) {
                expectedString << ", ";
            }

            expectedString << it->first << ": " << it->second;
        }

        // ASSERT_EQ(stringDefault, expectedString);
    }//TEST_F(E2EBenchmarkConfigPerRunTest, getStrLogicalSrcToNumberOfPhysicalSrcTest)

    TEST_F(E2EBenchmarkConfigPerRunTest, generateMapsLogicalSrcPhysicalSourcesTest) {
        std::vector<std::map<std::string, uint64_t>> expectedMap;

        // set default E2EBenchmarkConfigPerRun
        // call function generateMapsLogicalSrcPhysicalSources()

        auto logicalSourceNode = yamlConfig["logicalSources"];
        auto maxNoExperiments = 0UL;
        for (auto entry = logicalSourceNode.Begin(); entry != logicalSourceNode.End(); entry++) {
            auto node = (*entry).second;
            if (!node["numberOfPhysicalSources"].IsNone()) {
                auto tmpVec = NES::Util::splitWithStringDelimiter<uint64_t>(node["numberOfPhysicalSources"].As<std::string>(), ",");
                maxNoExperiments = std::max(maxNoExperiments, tmpVec.size());
            }
        }

        for (auto curExp = 0UL; curExp < maxNoExperiments; ++curExp) {
            std::map<std::string, uint64_t> map;
            for (auto entry = logicalSourceNode.Begin(); entry != logicalSourceNode.End(); entry++) {
                auto logicalSourceName = (*entry).first;
                if (map.contains(logicalSourceName)) {
                    NES_THROW_RUNTIME_ERROR("Logical source name has to be unique! "
                                            << logicalSourceName << " is duplicated!");
                }

                auto node = (*entry).second;
                auto value = 1UL;
                if (!node["numberOfPhysicalSources"].IsNone()) {
                    auto tmpVec = NES::Util::splitWithStringDelimiter<uint64_t>(node["numberOfPhysicalSources"].As<std::string>(), ",");
                    value = tmpVec[curExp];
                }

                map[logicalSourceName] = value;
            }
        }

        // ASSERT_EQ(defaultMap.size(), expectedMap.size());
        // ASSERT_TRUE(memcmp(defaultMap, expectedMap, defaultMap.size()) == 0);
    }//TEST_F(E2EBenchmarkConfigPerRunTest, generateMapsLogicalSrcPhysicalSourcesTest)
}//namespace NES::Benchmark