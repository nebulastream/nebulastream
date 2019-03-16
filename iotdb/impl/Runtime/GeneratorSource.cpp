/*
 * GeneratorSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Runtime/GeneratorSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::YSBGeneratorSource);

#include <iostream>
#include <Core/TupleBuffer.hpp>
namespace iotdb {

YSBGeneratorSource::YSBGeneratorSource()
{

}

YSBGeneratorSource::YSBGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process, size_t pCampaingCnt)
      : DataSource(schema), functor(pCampaingCnt){
	  this->num_buffers_to_process = pNum_buffers_to_process;
	  this->numberOfCampaings = pCampaingCnt;
	}

TupleBufferPtr YSBGeneratorSource::receiveData() {
    //we wait until the buffer is filled
	TupleBufferPtr buf = functor();
	generatedTuples += buf->num_tuples;
	generatedBuffers++;
    return buf;
};


const std::string YSBGeneratorSource::toString() const{
    std::stringstream ss;
    ss << "YSBGeneratorSource(SCHEMA(" << schema.toString();
    ss << "), NUM_BUFFERS=" << num_buffers_to_process <<
    		" numberOfCampaings=" << numberOfCampaings << "))";
    return ss.str();
}
}
