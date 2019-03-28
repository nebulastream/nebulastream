
#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <Core/TupleBuffer.hpp>
#include <Util/ErrorHandling.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/vector.hpp>

namespace iotdb {

class DataSink {
  public:
    DataSink();
    DataSink(const Schema& schema);
    virtual ~DataSink();

    virtual void setup() = 0;
    virtual void shutdown() = 0;
    bool writeData(const std::vector<TupleBufferPtr>& input_buffers);
    bool writeData(const TupleBufferPtr input_buffer);
    virtual bool writeData(const std::vector<TupleBuffer*>& input_buffers);
    virtual bool writeData(const TupleBuffer* input_buffer) = 0;
    size_t getNumberOfProcessedBuffers() { return processedBuffer; }
    size_t getNumberOfProcessedTuples() { return processedTuples; }

    virtual const std::string toString() const = 0;
    const Schema& getSchema() const;
    void setSchema(const Schema& pSchema) { schema = pSchema; };

  protected:
    Schema schema;
    size_t processedBuffer;
    size_t processedTuples;

    friend class boost::serialization::access;
    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& schema;
        ar& processedBuffer;
        ar& processedTuples;
    }
};
typedef std::shared_ptr<DataSink> DataSinkPtr;

const DataSinkPtr createTestSink();
const DataSinkPtr createPrintSink(std::ostream& out);
const DataSinkPtr createPrintSink(const Schema& schema, std::ostream& out);
const DataSinkPtr createBinaryFileSink(const std::string& path_to_file);
const DataSinkPtr createBinaryFileSink(const Schema& schema, const std::string& path_to_file);
const DataSinkPtr createRemoteTCPSink(const Schema& schema, const std::string& server_ip, int port);
const DataSinkPtr createZmqSink(const Schema& schema, const std::string& host, const uint16_t port);
const DataSinkPtr createYSBPrintSink();

} // namespace iotdb
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::DataSink)
#endif // INCLUDE_DATASINK_H_
