
#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_

#include <API/Schema.hpp>
#include <Core/TupleBuffer.hpp>
#include <Util/ErrorHandling.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>

namespace iotdb {

class DataSink {
public:
	DataSink(const Schema &schema);
	virtual ~DataSink();

	virtual void setup() = 0;
	virtual void shutdown() = 0;
	bool writeData(const std::vector<TupleBufferPtr> &input_buffers);
	bool writeData(const TupleBufferPtr input_buffer);
	virtual bool writeData(const std::vector<TupleBuffer*> &input_buffers);
	virtual bool writeData(const TupleBuffer* input_buffer) = 0;
	size_t getNumberOfProcessedBuffers(){return processedBuffer;}
	size_t getNumberOfProcessedTuples(){return processedTuples;}

	virtual const std::string toString() const = 0;
	const Schema &getSchema() const;

	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
		ar & schema;
		ar & processedBuffer;
		ar & processedTuples;
	}

protected:
  Schema schema;
  size_t processedBuffer;
  size_t processedTuples;

  friend class boost::serialization::access;
  DataSink(){};

};
typedef std::shared_ptr<DataSink> DataSinkPtr;

const DataSinkPtr createTestSink();
const DataSinkPtr createBinaryFileSink(const Schema &schema, const std::string &path_to_file);
const DataSinkPtr createRemoteTCPSink(const Schema &schema, const std::string &server_ip, int port);
const DataSinkPtr createZmqSink(const Schema &schema, const std::string &host, const uint16_t port);
const DataSinkPtr createYSBPrintSink();

} // namespace iotdb
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::DataSink)
#endif // INCLUDE_DATASINK_H_
