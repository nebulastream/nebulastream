/*
 * DataSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_DATASOURCE_H_
#define INCLUDE_DATASOURCE_H_

#include <API/Schema.hpp>
#include <Core/TupleBuffer.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>
#include <thread>

namespace iotdb {

class DataSource {
  public:
    DataSource(const Schema& schema);

    void start();
    void stop();
    void run();

    virtual TupleBufferPtr receiveData() = 0;
    virtual const std::string toString() const = 0;
    void submitWork(const TupleBuffer&);
    const Schema& getSchema() const;

    virtual bool isRunning();
    virtual ~DataSource();

    // debugging
    void setNumBuffersToProcess(size_t cnt) { num_buffers_to_process = cnt; };
    size_t getNumberOfGeneratedTuples() { return generatedTuples; };
    size_t getNumberOfGeneratedBuffers() { return generatedBuffers; };
    const std::string getSourceSchema() { return schema.toString(); };

  private:
    bool run_thread;
    std::thread thread;

  protected:
    DataSource();
    friend class boost::serialization::access;
    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& run_thread;
        //		ar & thread;
        ar& num_buffers_to_process;
        ar& generatedTuples;
        ar& generatedBuffers;
        ar& schema;
    }
    size_t num_buffers_to_process;
    size_t generatedTuples;
    size_t generatedBuffers;
    Schema schema;
};

typedef std::shared_ptr<DataSource> DataSourcePtr;

const DataSourcePtr createTestSource();
const DataSourcePtr createYSBSource(size_t bufferCnt, size_t campaingCnt, bool preGen);

const DataSourcePtr createZmqSource(const Schema& schema, const std::string& host, const uint16_t port);
const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
// BOOST_SERIALIZATION_ASSUME_ABSTRACT(iotdb::DataSource)
BOOST_CLASS_EXPORT_KEY(iotdb::DataSource)
#endif /* INCLUDE_DATASOURCE_H_ */
