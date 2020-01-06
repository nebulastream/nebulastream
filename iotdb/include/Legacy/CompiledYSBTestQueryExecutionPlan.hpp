#ifndef CompiledYSBTestQueryExecutionPlan_HPP_
#define CompiledYSBTestQueryExecutionPlan_HPP_
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "../NodeEngine/TupleBuffer.hpp"
#include "../SourceSink/DataSource.hpp"
#include "Windows/WindowHandler.hpp"
#include "../YSB_legacy/YSBWindow.hpp"

namespace iotdb{
struct __attribute__((packed)) ysbRecord {
      uint8_t user_id[16];
      uint8_t page_id[16];
      uint8_t campaign_id[16];
      char event_type[9];
      char ad_type[9];
      int64_t current_ms;
      uint32_t ip;

      ysbRecord(){
        event_type[0] = '-';//invalid record
        current_ms = 0;
        ip = 0;
      }

      ysbRecord(const ysbRecord& rhs)
      {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&event_type, &rhs.event_type, 9);
        memcpy(&ad_type, &rhs.ad_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.current_ms;
      }

    };//size 78 bytes

class CompiledYSBTestQueryExecutionPlan : public HandCodedQueryExecutionPlan{
public:
    uint64_t count;
    uint64_t sum;

    CompiledYSBTestQueryExecutionPlan();

    bool firstPipelineStage(const TupleBuffer&);

    union tempHash
    {
        uint64_t value;
        char buffer[8];
    };


    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf);
private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive & ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<HandCodedQueryExecutionPlan>(*this);
    }
};
typedef std::shared_ptr<CompiledYSBTestQueryExecutionPlan> CompiledYSBTestQueryExecutionPlanPtr;
}
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::CompiledYSBTestQueryExecutionPlan)
//#include <boost/serialization/export.hpp>
//#include <boost/archive/text_iarchive.hpp>
//#include <boost/archive/text_oarchive.hpp>
//BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::CompiledYSBTestQueryExecutionPlan);

#endif /* TESTS_QEPS_TEST_HPP_ */
