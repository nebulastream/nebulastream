#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class FileOutputSink : public DataSink {

  public:
    FileOutputSink(const Schema& schema);
    ~FileOutputSink();

    bool writeData(const std::vector<TupleBufferPtr>& input_buffers) override;
    bool writeData(const TupleBufferPtr input_buffers) override;

    const std::string toString() const override;

  private:
};
} // namespace iotdb

#endif // ZMQSINK_HPP
