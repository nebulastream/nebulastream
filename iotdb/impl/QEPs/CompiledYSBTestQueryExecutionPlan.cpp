#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <QEPs/CompiledYSBTestQueryExecutionPlan.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::CompiledYSBTestQueryExecutionPlan);

namespace iotdb{

CompiledYSBTestQueryExecutionPlan::CompiledYSBTestQueryExecutionPlan()
    : HandCodedQueryExecutionPlan(), count(0), sum(0){
}

bool CompiledYSBTestQueryExecutionPlan::firstPipelineStage(const TupleBuffer&){
    return false;
}
bool CompiledYSBTestQueryExecutionPlan::executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
{
    ysbRecord* tuples = (ysbRecord*) buf->buffer;
    size_t lastTimeStamp = time(NULL);
    size_t current_window = 0;
    char key[] = "view";
    size_t windowSizeInSec = 1;
    size_t campaingCnt = 10;
    YSBWindow* window = (YSBWindow*)this->getWindows()[0].get();
    std::atomic<size_t>** hashTable = window->getHashTable();
    for(size_t i = 0; i < buf->num_tuples; i++)
    {
        if(strcmp(key,tuples[i].event_type) != 0)
        {
            continue;
        }
        size_t timeStamp = time(NULL);

        if(lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0)
        {
            //increment to new window
            if(current_window == 0)
                current_window = 1;
            else
                current_window = 0;

            if(hashTable[current_window][campaingCnt] != timeStamp)
            {
                atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
                window->print();
                std::fill(hashTable[current_window], hashTable[current_window]+campaingCnt, 0);
                //memset(myarray, 0, N*sizeof(*myarray)); // TODO: is it faster?
            }

            //TODO: add output result
            lastTimeStamp = timeStamp;
        }

    //consume one tuple
        tempHash hashValue;
        hashValue.value = *(((uint64_t*) tuples[i].campaign_id) + 1);
        uint64_t bucketPos = (hashValue.value * 789 + 321)% campaingCnt;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
    }
    IOTDB_DEBUG("task " << this << " finished processing")
}
}
