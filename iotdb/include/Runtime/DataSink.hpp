
#ifndef INCLUDE_DATASINK_H_
#define INCLUDE_DATASINK_H_


#include <Core/TupleBuffer.hpp>
#include <API/Schema.hpp>

namespace iotdb {

class DataSink {
public:
    DataSink(const Schema& schema);
    virtual ~DataSink();

    virtual bool writeData(const std::vector<TupleBuffer*>& input_buffers) = 0;
    virtual const std::string toString() const = 0;
    const Schema& getSchema() const;

protected:
    Schema schema;

};
typedef std::shared_ptr<DataSink> DataSinkPtr;

const DataSinkPtr createTestSink();
const DataSinkPtr createBinaryFileSink(const Schema& schema, const std::string& path_to_file);
const DataSinkPtr createRemoteTCPSink(const Schema& schema, const std::string& server_ip, int port);
}

#endif //INCLUDE_DATASINK_H_
