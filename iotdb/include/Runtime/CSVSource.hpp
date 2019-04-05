#ifndef INCLUDE_CSVSOURCE_H_
#define INCLUDE_CSVSOURCE_H_

#include <Core/TupleBuffer.hpp>
#include <Runtime/DataSource.hpp>
#include <fstream>

namespace iotdb {

class CSVSource : public DataSource {
  public:
    CSVSource(const Schema& schema, const std::string& file_path, const uint64_t& num_tuples_to_process=-1);
    TupleBufferPtr receiveData();
    const std::string toString() const;
    void fillBuffer(TupleBuffer&);

  private:
    std::ifstream input;
    const std::string file_path;
    uint64_t num_tuples_to_process;
    // helper variables
    int64_t file_size;
    size_t tuple_size;
};
} // namespace iotdb

#endif /* INCLUDE_CSVSOURCE_H_ */
