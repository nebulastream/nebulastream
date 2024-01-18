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

#ifndef NES_BENCHMARK_INCLUDE_E2E_E2ESINGLERUN_HPP_
#define NES_BENCHMARK_INCLUDE_E2E_E2ESINGLERUN_HPP_

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

/**
 * @brief this class encapsulates a single benchmark run
 */
class E2ESingleRun {

    static constexpr auto defaultStopQueryTimeout = std::chrono::seconds(30);
    static constexpr auto defaultStartQueryTimeout = std::chrono::seconds(180);
    static constexpr auto sleepDuration = std::chrono::milliseconds(250);

  public:
    /**
     * @brief generates a E2ESingleRun object
     * @param configPerRun
     * @param configOverAllRuns
     * @param portOffSet
     * @param restPort
     */
    explicit E2ESingleRun(E2EBenchmarkConfigPerRun& configPerRun,
                          E2EBenchmarkConfigOverAllRuns& configOverAllRuns,
                          uint16_t rpcPort,
                          uint16_t restPort);

    /**
     * @brief destructs this object and clears all buffer managers and data providers, as well as the coordinator
     */
    virtual ~E2ESingleRun();

    /**
     * @brief this method takes care of running this single experiment.
     * So it will create the configurations, create the sources, ...
     */
    void run();

    /**
     * @brief Getter for the coordinator config
     * @return Returns the coordinatorConfiguration
     */
    [[nodiscard]] const CoordinatorConfigurationPtr& getCoordinatorConf() const;

    /**
    * @brief sets up the coordinator config and worker config
    */
    void setupCoordinatorConfig();

    /**
     * @brief creates all sources and the data generator and provider for each
     */
    void createSources();
    /**
     * @brief Submits all queries to the coordinator
     * @param queryService The query service to use
     * @param queryCatalog The query catalog to use
     */
    void submitQueries(RequestServicePtr queryService, QueryCatalogServicePtr queryCatalog);

    /**
     * @brief starts everything necessary for running the queries and measures for a single query
     */
    void runQueries();

    /**
     * @brief stops the queries and everything else, such as coordinator
     */
    void stopQueries();

    /**
     * @brief writes the measurement to the csv file
     */
    void writeMeasurementsToCsv();

    /**
     * @brief Getter for the measurements of this run
     * @return Reference to the measurements
     */
    Measurements::Measurements& getMeasurements();

  private:
    /**
     * @brief Creates either KafkaSourceType or LambdaSourceType depending on the data generator.
     * Also creates a data provider for LambdaSourceType.
     * @param logicalSourceName
     * @param physicalSourceName
     * @param createdBuffers
     * @param sourceCnt
     * @param groupId
     * @param generator
     * @return KafkaSourceType or LambdaSourceType
     */
    PhysicalSourceTypePtr createPhysicalSourceType(std::string logicalSourceName,
                                                   std::string physicalSourceName,
                                                   std::vector<Runtime::TupleBuffer>& createdBuffers,
                                                   size_t sourceCnt,
                                                   uint64_t groupId,
                                                   std::string& generator);

    /**
     * @brief Collects the measurements for every query. It stops after numMeasurementsToCollect iterations.
     */
    void collectMeasurements();

    /**
     * @brief This method is used for waiting until the query gets into running status or a timeout occurs
     * @param queryId: the query id to check for
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into running status else false
     */
    static bool waitForQueryToStart(QueryId queryId,
                                    const QueryCatalogServicePtr& queryCatalogService,
                                    std::chrono::seconds timeoutInSec = defaultStartQueryTimeout);

    /**
     * @brief This method is used for waiting until the query gets into stopped status or a timeout occurs
     * @param queryId: the query id to check for
     * @param queryCatalogService: the catalog to look into for status change
     * @param timeoutInSec: time to wait before stop checking
     * @return true if query gets into stopped status else false
     */
    static bool waitForQueryToStop(QueryId queryId,
                                   const QueryCatalogServicePtr& queryCatalogService,
                                   std::chrono::seconds timeoutInSec = defaultStopQueryTimeout);

    /**
     * @brief Prints query subplan statistics to std::cout
     * @param timestamp
     * @param subPlanStatistics
     * @param processedTasks
     * @param outStream
     */
    static void printQuerySubplanStatistics(uint64_t timestamp,
                                            const Runtime::QueryStatisticsPtr& subPlanStatistics,
                                            size_t processedTasks,
                                            std::ostream& outStream = std::cout);

    E2EBenchmarkConfigPerRun& configPerRun;
    E2EBenchmarkConfigOverAllRuns& configOverAllRuns;
    int rpcPortSingleRun;
    int restPortSingleRun;
    NES::Configurations::CoordinatorConfigurationPtr coordinatorConf;
    NES::NesCoordinatorPtr coordinator;
    std::vector<DataProvision::DataProviderPtr> allDataProviders;
    std::vector<NES::Runtime::BufferManagerPtr> allBufferManagers;
    Measurements::Measurements measurements;
    std::vector<QueryId> submittedIds;
};
}// namespace NES::Benchmark

#endif// NES_BENCHMARK_INCLUDE_E2E_E2ESINGLERUN_HPP_
