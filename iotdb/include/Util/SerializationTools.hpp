#ifndef SERIALIZATIONTOOLS_HPP
#define SERIALIZATIONTOOLS_HPP

#include <Actors/ExecutableTransferObject.hpp>
#include <string>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/serialization/access.hpp>

using std::string;

namespace NES {

/**\brief:
 * Here are the methods which are used for boost serialization
 */
class SerializationTools {
  public:
    /**
   * @brief converts a predicate into a serialized Boost string
   * @param PredicatePtr predicate to be serialized
   * @return the string serialized object
   */
    static string ser_predicate(const PredicatePtr& predicate) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << predicate;
        }
        return s;
    }

    /**
   * @brief converts a Schema into a serialized Boost string
   * @param Schema schema to be serialized
   * @return the string serialized object
   */
    static string ser_schema(SchemaPtr schema) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << schema;
        }
        return s;
    }

    /**
   * @brief converts a sink into a serialized Boost string
   * @param DataSinkPtr sink to be serialized
   * @return the string serialized object
   */
    static string ser_sink(const DataSinkPtr& sink) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << sink;
        }
        return s;
    }

    /**
   * @brief converts a source into a serialized Boost string
   * @param DataSourcePtr source to be serialized
   * @return the string serialized object
   */
    static string ser_source(const DataSourcePtr& source) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << source;
        }
        return s;
    }

    /**
   * @brief converts an operator tree into a serialized Boost string
   * @param OperatorPtr operator tree to be serialized
   * @return the string serialized object
   */
    static string ser_operator(const OperatorPtr& op) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << op;
        }
        return s;
    }

    /**
   * @brief converts an ExecutableTransferObject into a serialized Boost string
   * @param ExecutableTransferObject to be serialized
   * @return the string serialized object
   */
    static string ser_eto(const ExecutableTransferObject& eto) {
        std::string s;
        {
            namespace io = boost::iostreams;
            io::stream<io::back_insert_device<std::string>> os(s);

            boost::archive::text_oarchive archive(os);
            archive << eto;
        }
        return s;
    }

    /**
   * @brief parses Boost string serialized Predicate into an PredicatePtr object
   * @param string boost serialized string object
   * @return the deserialized object
   */
    static PredicatePtr parse_predicate(const string& s) {
        PredicatePtr pred;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> pred;
        }
        return pred;
    }

    /**
   * @brief parses Boost string serialized Schema into an Schema object
   * @param string boost serialized string object
   * @return the deserialized object
   */
    static SchemaPtr parse_schema(const string& s) {
        SchemaPtr schema;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> schema;
        }
        return schema;
    }

    /**
   * @brief parses Boost string serialized DataSink into an DataSinkPtr object
   * @param string boost serialized string object
   * @return the deserialized object
   */
    static DataSinkPtr parse_sink(const string& s) {
        DataSinkPtr sink;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> sink;
        }
        return sink;
    }

    /**
 * @brief parses Boost string serialized DataSource into an DataSourcePtr object
 * @param string boost serialized string object
 * @return the deserialized object
 */
    static DataSourcePtr parse_source(const string& s) {
        DataSourcePtr source;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> source;
        }
        return source;
    }

    /**
   * @brief parses Boost string serialized Operator tree into an OparotrPtr object
   * @param string boost serialized string object
   * @return the deserialized object
   */
    static OperatorPtr parse_operator(const string& s) {
        OperatorPtr op;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> op;
        }
        return op;
    }

    /**
   * @brief parses Boost string serialized ExecutableTransferObject
   * @param string boost serialized string object
   * @return the deserialized object
   */
    static ExecutableTransferObject parse_eto(const string& s) {
        ExecutableTransferObject eto;
        {
            namespace io = boost::iostreams;
            io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
            boost::archive::text_iarchive archive(is);
            archive >> eto;
        }
        return eto;
    }
};

}// namespace NES

#endif//SERIALIZATIONTOOLS_HPP
