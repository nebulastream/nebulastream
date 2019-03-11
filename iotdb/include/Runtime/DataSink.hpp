
#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <Core/TupleBuffer.hpp>
#include <Util/ErrorHandling.hpp>
namespace iotdb {

class DataSink {
public:
  DataSink(const Schema &schema);
  virtual ~DataSink();

  bool writeData(const std::vector<TupleBuffer*> &input_buffers){
	  IOTDB_NOT_IMPLEMENTED("not impl");
  };

  virtual void setup() = 0;
  virtual void shutdown() = 0;
  virtual bool writeData(const std::vector<TupleBufferPtr> &input_buffers) = 0;
  virtual bool writeData(const TupleBufferPtr input_buffer) = 0;

  virtual const std::string toString() const = 0;
  const Schema &getSchema() const;

protected:
  Schema schema;
};
typedef std::shared_ptr<DataSink> DataSinkPtr;

const DataSinkPtr createTestSink();
const DataSinkPtr createBinaryFileSink(const Schema &schema, const std::string &path_to_file);
const DataSinkPtr createRemoteTCPSink(const Schema &schema, const std::string &server_ip, int port);
const DataSinkPtr createZmqSink(const Schema &schema, const std::string &host, const uint16_t port,
                                const std::string &topic);

const DataSinkPtr createYSBPrintSink(const Schema &schema);
} // namespace iotdb

#endif // INCLUDE_DATASINK_H_
