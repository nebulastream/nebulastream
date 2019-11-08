#ifndef FILEOUTPUTSINK_HPP
#define FILEOUTPUTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class FileOutputSink : public DataSink {

  public:
    FileOutputSink();
    FileOutputSink(const Schema& schema);
    ~FileOutputSink();

    virtual void setup() override {}
    virtual void shutdown() override {}

    bool writeData(const TupleBufferPtr input_buffer);
    const std::string toString() const override;

  private:
};
} // namespace iotdb

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::FileOutputSink)

#endif // FILEOUTPUTSINK_HPP
