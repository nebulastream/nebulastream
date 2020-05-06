#ifndef NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#define NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
#include <string>

namespace NES {

class QueryStatistics {
  public:
    QueryStatistics() : processedTasks(0),
                        processedTuple(0),
                        processedBuffers(0){};

    /**
     * @brief getter for processedTasks
     * @return processedTasks
     */
    const std::atomic<size_t>& getProcessedTasks() const {
        return processedTasks;
    }

    /**
   * @brief getter for processedTuple
   * @return processedTuple
   */
    const std::atomic<size_t>& getProcessedTuple() const {
        return processedTuple;
    }

    /**
   * @brief getter for processedBuffers
   * @return processedBuffers
   */
    const std::atomic<size_t>& getProcessedBuffers() const {
        return processedBuffers;
    }

    /**
    * @brief setter for processedTasks
    * @return processedTasks
    */
    void setProcessedTasks(const std::atomic<size_t>& processedTasks) {
        this->processedTasks = processedTasks.load();
    }

    /**
   * @brief setter for processedTuple
   * @return processedTuple
   */
    void setProcessedTuple(const std::atomic<size_t>& processedTuple) {
        this->processedTuple = processedTuple.load();
    }

    /**
  * @brief increment processedBuffers
  */
    void incProcessedBuffers() {
        this->processedBuffers++;
    }

    /**
    * @brief increment processedTasks
    */
    void incProcessedTasks() {
        this->processedTasks++;
    }

    /**
   * @brief increment processedTuple
   */
    void incProcessedTuple(size_t tupleCnt) {
        this->processedTuple += tupleCnt;
    }

    /**
  * @brief setter for processedBuffers
  * @return processedBuffers
  */
    void setProcessedBuffers(const std::atomic<size_t>& processedBuffers) {
        this->processedBuffers = processedBuffers.load();
    }

    std::string getQueryStatisticsAsString() {
        std::stringstream ss;
        ss << "processedTasks=" << processedTasks;
        ss << " processedTuple=" << processedTuple;
        ss << " processedBuffers=" << processedBuffers;
        return ss.str();
    }

  private:
    std::atomic<size_t> processedTasks;
    std::atomic<size_t> processedTuple;
    std::atomic<size_t> processedBuffers;
};
typedef std::shared_ptr<QueryStatistics> QueryStatisticsPtr;

}// namespace NES

#endif//NES_INCLUDE_NODEENGINE_QUERYSTATISTICS_HPP_
