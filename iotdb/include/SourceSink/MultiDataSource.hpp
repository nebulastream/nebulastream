//
// Created by xchatziliadis on 27.01.20.
//

#ifndef NES_INCLUDE_SOURCESINK_MULTIMultiDataSource_HPP_
#define NES_INCLUDE_SOURCESINK_MULTIMultiDataSource_HPP_

#include <API/Schema.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <thread>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {

/**
 * @brief Base class for all data sources in NES
 */
class MultiDataSource {
 public:
  /**
   * @brief public constructor for data source
   * @Note the number of buffers to process is set to UINT64_MAX and the value is needed
   * by some test to produce a deterministic behavior
   */
  MultiDataSource();

  /**
   * @brief method to start the source.
   * 1.) check if bool running is true, if true return if not start source
   * 2.) start new thread with running_routine
   */
  bool start();

  /**
   * @brief method to stop the source.
   * 1.) check if bool running is false, if false return, if not stop source
   * 2.) stop thread by join
   */
  bool stop();

  /**
   * @brief running routine while source is active
   * 1.) check if running is still true
   * 2.) check if max number of buffer to produced is reached (num_buffers_to_process)
   * 3.) If not call receiveData in a blocking fashion
   * 4.) If call returns and a buffer is there to process, add a task to the dispatcher
   */
  void running_routine();

  /**
   * @brief virtual function to receive a buffer
   * @Note this function is overwritten by the particular data source
   * @return returns a tuple buffer
   */
  virtual TupleBufferPtr receiveData() = 0;

  /**
   * @brief virtual function to get a string describing the particular source
   * @Note this function is overwritten by the particular data source
   * @return string with name and additional information about the source
   */
  virtual const std::string toString() const = 0;

  /**
   * @brief get source Type
   * @return
   */
  virtual SourceType getType() const = 0;

  /**
   * @brief debug function for testing to test if source is running
   * @return bool indicating if source is running
   */
  virtual bool isRunning();

  /**
   * @brief debug function for testing to set different buffer counts to be processed
   * @param number of buffers to be processed
   */
  void setNumBuffersToProcess(size_t cnt);

  /**
   * @brief debug function for testing to get number of generated tuples
   * @return number of generated tuples
   */
  size_t getNumberOfGeneratedTuples();

  /**
   * @brief debug function for testing to get number of generated buffer
   * @return number of generated buffer
   */
  size_t getNumberOfGeneratedBuffers();

  /**
   * @brief Internal destructor to make sure that the data source is stopped before deconstrcuted
   * @Note must be public because of boost serialize
   */
  virtual ~MultiDataSource();

 private:
  //bool indicating if the source is currently running
  bool running;
  std::thread thread;

 protected:
  size_t generatedTuples;
  size_t generatedBuffers;
  size_t num_buffers_to_process;

 private:
  friend class boost::serialization::access;

  template<class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar & num_buffers_to_process;
    ar & generatedTuples;
    ar & generatedBuffers;
  }
};

typedef std::shared_ptr<MultiDataSource> MultiDataSourcePtr;

}  // namespace NES

#endif //NES_INCLUDE_SOURCESINK_MULTIMultiDataSource_HPP_
