#include <cassert>
#include <iostream>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>
#include <API/Types/DataTypes.hpp>
#include <Util/UtilityFunctions.hpp>
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


        Stream def = Stream("default", schema);

        InputQuery& query = InputQuery::from(def)
                .filter(def["value"]  > 42)
                .windowByKey(def["value"].getAttributeField(),TumblingWindow::of(Seconds(10)), Sum::on(def["value"]))
                .print(std::cout);

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

        Stream def = Stream("default", schema);

        AttributeField mappedField("id", BasicType::UINT64);

        InputQuery& query = InputQuery::from(def)
                .map(*schema[0], def["value"] + schema[1])
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

        InputQueryPtr inputQuery = UtilityFunctions::createQueryFromCodeString(code.str());
    }

} // namespace iotdb

int main(int argc, const char *argv[]) {

    iotdb::Dispatcher::instance();
    iotdb::createQueryFilter();

    //iotdb::createQueryMap();
    //iotdb::createQueryString();

    return 0;
}
