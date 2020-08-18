#ifndef NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#define NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#include <atomic>
#include <memory>
#include <string>
namespace NES {

class QueryStatistics {
  public:
    QueryStatistics() : processedTasks(0),
                        processedTuple(0),
                        processedBuffers(0),
                        processedWatermarks(0){};

    /**
     * @brief getter for processedTasks
     * @return processedTasks
     */
    const std::atomic<uint64_t> getProcessedTasks() const;

    /**
   * @brief getter for processedTuple
   * @return processedTuple
   */
    const std::atomic<uint64_t> getProcessedTuple() const;

    /**
   * @brief getter for processedBuffers
   * @return processedBuffers
   */
    const std::atomic<uint64_t> getProcessedBuffers() const;

    /**
    * @brief getter for processedWatermarks
    * @return processedBuffers
    */
    const std::atomic<uint64_t> getProcessedWatermarks() const;

    /**
    * @brief setter for processedTasks
    * @return processedTasks
    */
    void setProcessedTasks(const std::atomic<uint64_t>& processedTasks);

    /**
   * @brief setter for processedTuple
   * @return processedTuple
   */
    void setProcessedTuple(const std::atomic<uint64_t>& processedTuple);

    /**
  * @brief increment processedBuffers
  */
    void incProcessedBuffers();

    /**
    * @brief increment processedTasks
    */
    void incProcessedTasks();

    /**
   * @brief increment processedTuple
   */
    void incProcessedTuple(uint64_t tupleCnt);

    /**
    * @brief increment processedWatermarks
    */
    void incProcessedWatermarks();

    /**
  * @brief setter for processedBuffers
  * @return processedBuffers
  */
    void setProcessedBuffers(const std::atomic<uint64_t>& processedBuffers);

    std::string getQueryStatisticsAsString();

  private:
    std::atomic<uint64_t> processedTasks;
    std::atomic<uint64_t> processedTuple;
    std::atomic<uint64_t> processedBuffers;
    std::atomic<uint64_t> processedWatermarks;
};
typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
