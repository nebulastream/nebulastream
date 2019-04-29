/*
 * CSVSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/CSVSource.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <boost/algorithm/string.hpp>


namespace iotdb {

    CSVSource::CSVSource(const Schema& schema, const std::string& _file_path, const uint64_t& _num_tuples_to_process, const std::string& delimiter)
        : DataSource(schema), input(std::ifstream(_file_path.c_str())), file_path(_file_path),
          num_tuples_to_process(_num_tuples_to_process), delimiter(delimiter)
    {
        input.seekg(0, input.end);
        file_size = input.tellg();
        if (file_size == -1) {
            std::cerr << "ERROR: File " << _file_path << " is corrupted";
            std::abort();
        }
        input.seekg(0, input.beg);
        tuple_size = schema.getSchemaSize();
        generatedTuples = 0;
        generatedBuffers = 0;
    }

    TupleBufferPtr CSVSource::receiveData()
    {
        TupleBufferPtr buf = BufferManager::instance().getBuffer();
        fillBuffer(*buf);
        return buf;
    }

    const std::string CSVSource::toString() const
    {
        std::stringstream ss;
        ss << "CSV_SOURCE(SCHEMA(" << schema.toString() << "), FILE=" << file_path << ")";
        ss << ", NUM_TUPLES=" << num_tuples_to_process << "))";
        return ss.str();
    }

    void CSVSource::fillBuffer(TupleBuffer& buf)
    {
        /* while(generated_tuples < num_tuples_to_process) */
        /* read <buf.buffer_size> bytes data from file into buffer */
        /* advance internal file pointer, if we reach the file end, set to file begin */

        uint64_t generated_tuples_this_pass = buf.buffer_size / tuple_size;

        std::string line;
        std::vector<std::string> tokens;
        uint64_t i = 0;
        while (i < generated_tuples_this_pass) {
            std::getline(input, line);

            if (input.tellg() == file_size) {
                input.seekg(0, input.beg);
            }
            boost::algorithm::split(tokens, line, boost::is_any_of(this->delimiter));
            size_t offset = 0;
            for (size_t j = 0; j < schema.getSize(); j ++) {
                auto field = schema[j];
                // std::cout << field->toString() << ": " << tokens[j] << ", ";
                size_t field_size = field->getFieldSize();
                memcpy((char *)buf.buffer + offset + i*tuple_size, tokens[j].c_str(), field_size);
                offset += field_size;
            }
            // std::cout << std::endl;
            i ++;
        }
        generatedTuples += generated_tuples_this_pass;
        generatedBuffers ++;
    }
} // namespace iotdb
