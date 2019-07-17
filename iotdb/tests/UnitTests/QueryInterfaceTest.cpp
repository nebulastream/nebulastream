

#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>

namespace iotdb {

    class SelectionDataGenFunctor {
    public:
        SelectionDataGenFunctor() {}

        struct __attribute__((packed)) InputTuple {
            uint32_t id;
            uint32_t value;
        };


        TupleBufferPtr operator()() {
            // 10 tuples of size one
            TupleBufferPtr buf = BufferManager::instance().getBuffer();
            uint64_t tupleCnt = buf->buffer_size / sizeof(InputTuple);

            assert(buf->buffer != NULL);

            InputTuple *tuples = (InputTuple *) buf->buffer;
            for (uint32_t i = 0; i < tupleCnt; i++) {
                tuples[i].id = i;
                tuples[i].value = i * 2;
            }
            buf->tuple_size_bytes = sizeof(InputTuple);
            buf->num_tuples = tupleCnt;
            return buf;
        }
    };

    void createQuery() {
        // define config
        Config config = Config::create();

        Environment env = Environment::create(config);

//    Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);
        Schema schema = Schema::create()
                .addField("id", BasicType::UINT32)
                .addField("value", BasicType::UINT32);

        /** \brief create a source using the following functions:
         * const DataSourcePtr createTestSource();
         * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
         * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);

        InputQuery &query = InputQuery::from("cars", schema)
                .filter(Field("rents") <= 10)
                .map(Field("revenue"), Field("price") - Field("tax"))
                .windowByKey(
                        Field("id"),
                        TumblingWindow::of(Seconds(10)),
                        Sum(Field("revenue")).as(Field("revenuePerCar"))
                )
                .print(std::cout);
  */

        InputQuery &query = InputQuery::from("cars", schema)
                .filter(Field("value") <= 10)
                .print(std::cout);
        env.printInputQueryPlan(query);
        env.executeQuery(query);

        // AttributeFieldPtr attr = schema[0];
    }

    void createQueryString() {

        std::stringstream code;
        code << "Config config = Config::create()"
                ".setBufferCount(2000)"
                ".setBufferSizeInByte(8*1024)"
                ".setNumberOfWorker(2);"
             << std::endl;

        code << "Schema schema = Schema::create().addField(\"test\",INT32);" << std::endl;

        code << "DataSourcePtr source = createTestSource();" << std::endl;

        code << "return InputQuery::from(\"test\", schema).filter(Field(\"test\")==5)" << std::endl
             << "" << std::endl
             << ";" << std::endl;

        InputQuery q(createQueryFromCodeString(code.str()));
    }

} // namespace iotdb

int main(int argc, const char *argv[]) {

    iotdb::Dispatcher::instance();

    iotdb::createQuery();
    iotdb::createQueryString();

    return 0;
}
