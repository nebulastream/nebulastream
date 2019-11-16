

#include <cassert>
#include <iostream>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <NodeEngine/ThreadPool.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>
#include "../../include/CodeGen/DataTypes.hpp"
#include "../../include/NodeEngine/TupleBuffer.hpp"
#include "../../include/SourceSink/DataSource.hpp"
#include "../../include/SourceSink/GeneratorSource.hpp"

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
            uint64_t tupleCnt = buf->getNumberOfTuples();

            assert(buf->getBuffer() != NULL);

            InputTuple *tuples = (InputTuple *) buf->getBuffer();
            for (uint32_t i = 0; i < tupleCnt; i++) {
                tuples[i].id = i;
                tuples[i].value = i * 2;
            }
            buf->setTupleSizeInBytes(sizeof(InputTuple));
            buf->setNumberOfTuples(tupleCnt);
            return buf;
        }
    };

    void createQueryFilter() {
        // define config
        Config config = Config::create();

        Environment env = Environment::create(config);

//    Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);
        Schema schema = Schema::create()
                .addField("id", BasicType::UINT32)
                .addField("value", BasicType::UINT64);

        /** \brief create a source using the following functions:
         * const DataSourcePtr createTestSource();
         * const DataSourcePtr createBinaryFileSource(const Schema& schema, const std::string& path_to_file);
         * const DataSourcePtr createRemoteTCPSource(const Schema& schema, const std::string& server_ip, int port);

        InputQuery &query = InputQuery::from("cars", schema)
                .filter(Field("rents", Int) <= 10)
                .map(Field("revenue"), Field("price") - Field("tax"))
                .windowByKey(
                        Field("id"),
                        TumblingWindow::of(Seconds(10)),
                        Sum(Field("revenue")).as(Field("revenuePerCar"))
                )
                .print(std::cout);
  */

        Stream cars = Stream("cars", schema);

        InputQuery& query = InputQuery::from(cars)
                .filter(cars["value"]  > 42)
                .windowByKey(cars["value"].getAttributeField(),TumblingWindow::of(Seconds(10)), Sum::on(cars["value"]));

        env.printInputQueryPlan(query);
        env.executeQuery(query);

        // AttributeFieldPtr attr = schema[0];
    }

    void createQueryMap() {
        // define config
        Config config = Config::create();

        Environment env = Environment::create(config);

//    Config::create().withParallelism(1).withPreloading().withBufferSize(1000).withNumberOfPassesOverInput(1);
        Schema schema = Schema::create()
                .addField("id", BasicType::UINT32)
                .addField("value", BasicType::UINT64);

        Stream cars = Stream("cars", schema);

        AttributeField mappedField("id", BasicType::UINT64);

        InputQuery& query = InputQuery::from(cars)
                .map(*schema[0], cars["value"] + schema[1])
                .print(std::cout);
        env.printInputQueryPlan(query);
        env.executeQuery(query);

        // AttributeFieldPtr attr = schema[0];
    }

    void createQueryString() {

        std::stringstream code;

        code << "Schema schema = Schema::create().addField(\"test\",INT32);" << std::endl;
        code << "Stream testStream = Stream(\"test-stream\",schema);" << std::endl;
        code << "return InputQuery::from(testStream).filter(testStream[\"test\"]==5)" << std::endl
             << "" << std::endl
             << ";" << std::endl;

        InputQuery q(createQueryFromCodeString(code.str()));
    }

} // namespace iotdb

int main(int argc, const char *argv[]) {

    iotdb::Dispatcher::instance();
    iotdb::createQueryFilter();

    //iotdb::createQueryMap();
    //iotdb::createQueryString();

    return 0;
}
