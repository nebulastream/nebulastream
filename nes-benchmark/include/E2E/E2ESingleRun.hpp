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

#ifndef NES_E2ESINGLERUN_HPP
#define NES_E2ESINGLERUN_HPP

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <DataGeneration/DataGenerator.hpp>
#include <DataProvider/DataProvider.hpp>
#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <Measurements.hpp>
#include <vector>

namespace NES::Benchmark {

/*
 * @brief this class encapsulates a single benchmark run
 */
class E2ESingleRun {

    static constexpr auto stopQuerySleep = std::chrono::milliseconds(250);
    static constexpr auto stopQueryTimeoutInSec = std::chrono::seconds(30);
    static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(180);
    static constexpr auto sleepDuration = std::chrono::milliseconds(250);

  public:
    /**
     * @brief generates a E2ESingleRun object
     * @param configPerRun
     * @param configOverAllRuns
     * @param portOffSet
     */
    explicit E2ESingleRun(const E2EBenchmarkConfigPerRun& configPerRun,
                          const E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                          int portOffSet);

    /**
     * @brief destroying this object and taking care of
     */
    virtual ~E2ESingleRun();

    /**
     * @brief this method takes care of running this single experiment.
     * So it will create the configurations, create the sources, ...
     */
    void run();

  private:
    /**
     * @brief sets up the coordinator config and worker config
     */
    void setupCoordinatorConfig();

    /**
     * @brief creates all sources and the data generator and provider for each
     */
    void createSources();

    /**
     * @brief starts all everything necessary for running the query and measures for a single query
     */
    void runQuery();

    /**
     * @brief stops the query and everything else, such as coordinator
     */
    void stopQuery();

    /**
     * @brief writes the measurement to the csv file
     */
    void writeMeasurementsToCsv();

    /**
     * @brief This method is used for waiting till the query gets into running status or a timeout occurs
     * @param queryId : the query id to check for
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
    static bool waitForQueryToStart(QueryId queryId,
                                    const QueryCatalogServicePtr& queryCatalogService,
                                    std::chrono::seconds timeoutInSec = std::chrono::seconds(defaultStartQueryTimeout));

  private:
    E2EBenchmarkConfigPerRun configPerRun;
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    int portOffSet;
    NES::Configurations::CoordinatorConfigurationPtr coordinatorConf;
    NES::NesCoordinatorPtr coordinator;
    std::vector<DataProviding::DataProviderPtr> allDataProviders;
    std::vector<DataGeneration::DataGeneratorPtr> allDataGenerators;
    std::vector<NES::Runtime::BufferManagerPtr> allBufferManagers;
    Measurements::Measurements measurements;
    std::vector<QueryId> submittedIds;
};
}// namespace NES::Benchmark

#endif//NES_E2ESINGLERUN_HPP
