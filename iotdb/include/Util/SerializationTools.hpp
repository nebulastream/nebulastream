//
// Created by xchatziliadis on 01.11.19.
//

#ifndef IOTDB_SERIALIZATIONTOOLS_HPP
#define IOTDB_SERIALIZATIONTOOLS_HPP

#include <string>
#include <CodeGen/CodeGen.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/serialization/access.hpp>

using std::string;

namespace iotdb {

class SerializationTools {
 public:
  static string ser_predicate(const PredicatePtr &predicate) {
    std::string s;
    {
      namespace io = boost::iostreams;
      io::stream<io::back_insert_device<std::string>> os(s);

      boost::archive::text_oarchive archive(os);
      archive << predicate;
    }
    return s;
  }

  static string ser_schema(const Schema &schema) {
    std::string s;
    {
      namespace io = boost::iostreams;
      io::stream<io::back_insert_device<std::string>> os(s);

      boost::archive::text_oarchive archive(os);
      archive << schema;
    }
    return s;
  }

  static string ser_sink(const DataSinkPtr &sink) {
    std::string s;
    {
      namespace io = boost::iostreams;
      io::stream<io::back_insert_device<std::string>> os(s);

      boost::archive::text_oarchive archive(os);
      archive << sink;
    }
    return s;
  }

  static string ser_source(const DataSourcePtr &source) {
    std::string s;
    {
      namespace io = boost::iostreams;
      io::stream<io::back_insert_device<std::string>> os(s);

      boost::archive::text_oarchive archive(os);
      archive << source;
    }
    return s;
  }

  static string ser_operator(const OperatorPtr &op) {
    std::string s;
    {
      namespace io = boost::iostreams;
      io::stream<io::back_insert_device<std::string>> os(s);

      boost::archive::text_oarchive archive(os);
      archive << op;
    }
    return s;
  }

  static PredicatePtr parse_predicate(const string &s) {
    PredicatePtr pred;
    {
      namespace io = boost::iostreams;
      io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
      boost::archive::text_iarchive archive(is);
      archive >> pred;
    }
    return pred;
  }

  static Schema parse_schema(const string &s) {
    Schema schema;
    {
      namespace io = boost::iostreams;
      io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
      boost::archive::text_iarchive archive(is);
      archive >> schema;
    }
    return schema;
  }

  static DataSinkPtr parse_sink(const string &s) {
    DataSinkPtr sink;
    {
      namespace io = boost::iostreams;
      io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
      boost::archive::text_iarchive archive(is);
      archive >> sink;
    }
    return sink;
  }

  static DataSourcePtr parse_source(const string &s) {
    DataSourcePtr source;
    {
      namespace io = boost::iostreams;
      io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
      boost::archive::text_iarchive archive(is);
      archive >> source;
    }
    return source;
  }

  static OperatorPtr parse_operator(const string &s) {
    OperatorPtr op;
    {
      namespace io = boost::iostreams;
      io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
      boost::archive::text_iarchive archive(is);
      archive >> op;
    }
    return op;
  }

};

} // namespace iotdb

#endif //IOTDB_SERIALIZATIONTOOLS_HPP
