#ifndef INCLUDE_SOURCESINK_SOURCECREATOR_HPP_
#define INCLUDE_SOURCESINK_SOURCECREATOR_HPP_

#include <SourceSink/DataSource.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <SourceSink/GeneratorSource.hpp>

namespace NES {


class OneGeneratorSource : public GeneratorSource {
  public:
    OneGeneratorSource() = default;
    OneGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process) :
        GeneratorSource(schema, pNum_buffers_to_process) {
    }

  TupleBufferPtr receiveData() override;

 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & boost::serialization::base_object<GeneratorSource>(*this);
  }
};

/**
 * @brief function to create a test source which produces 10 tuples with value one based on a schema
 * @return a const data source pointer
 */
const DataSourcePtr createTestDataSourceWithSchema(const Schema& schema);

/**
 * @brief function to create a test source which produces 10 tuples with value one without a schema
 * @return a const data source pointer
 */
const DataSourcePtr createTestSourceWithoutSchema();

/**
 * @brief function to create a test source which produces tuples of the YSB benchmark
 * @return a const data source pointer
 */
const DataSourcePtr createYSBSource(size_t bufferCnt, size_t campaingCnt,
                                    bool preGen);

/**
 * @brief function to create an empty zmq source
 * @return a const data source pointer
 */
const DataSourcePtr createZmqSource(const Schema& schema,
                                    const std::string& host,
                                    const uint16_t port);

/**
 * @brief function to create a binary file source
 * @return a const data source pointer
 */
const DataSourcePtr createBinaryFileSource(const Schema& schema,
                                           const std::string& path_to_file);

/**
 * @brief function to create a csvfile source
 * @return a const data source pointer
 */
const DataSourcePtr createCSVFileSource(const Schema& schema,
                                        const std::string& path_to_file);
}
#endif /* INCLUDE_SOURCESINK_SOURCECREATOR_HPP_ */
