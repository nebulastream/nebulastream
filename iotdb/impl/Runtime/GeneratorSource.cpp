/*
 * GeneratorSource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <Runtime/Dispatcher.hpp>

#include <Runtime/GeneratorSource.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::YSBGeneratorSource);

#include <iostream>
#include <Core/TupleBuffer.hpp>
namespace iotdb {

YSBGeneratorSource::YSBGeneratorSource() : numberOfCampaings(0), preGenerated(false)
{

}

YSBGeneratorSource::YSBGeneratorSource(const Schema& schema,
		const uint64_t pNum_buffers_to_process,
		size_t pCampaingCnt, bool preGenerated)
      : DataSource(schema), functor(pCampaingCnt), preGenerated(preGenerated)
{
	  this->num_buffers_to_process = pNum_buffers_to_process;
	  this->numberOfCampaings = pCampaingCnt;
	  if(preGenerated)
	  {
		  copyBuffer = functor();
	  }
}


struct __attribute__((packed)) ysbRecord {
	  char user_id[16];
	  char page_id[16];
	  char campaign_id[16];
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

void generateTuple(ysbRecord* data, size_t campaingOffset, uint64_t campaign_lsb, uint64_t campaign_msb, size_t event_id)
{
		std::string events[] = {"view", "click", "purchase"};
		size_t currentID  = event_id % 3;

	  memcpy(data->campaign_id, &campaign_msb, 8);

	  uint64_t campaign_lsbr = campaign_lsb + campaingOffset;
	  memcpy(&data->campaign_id[8], &campaign_lsbr, 8);

	  const char* str = events[currentID].c_str();
	  strcpy(&data->ad_type[0], "banner78");
	  strcpy(&data->event_type[0], str);

	  auto ts = std::chrono::system_clock::now().time_since_epoch();
	  data->current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(ts).count();

	  data->ip = event_id;
}

void generate(ysbRecord* data, size_t generated_tuples_this_pass, size_t campaingCnt)
{
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen; //(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<size_t> diss(0, SIZE_MAX);

	size_t randomCnt = generated_tuples_this_pass/10;
	size_t* randomNumbers = new size_t[randomCnt];
	std::uniform_int_distribution<size_t> disi(0, campaingCnt);
	for(size_t i = 0; i < randomCnt; i++)
		randomNumbers[i] = disi(gen);

	uint64_t campaign_lsb, campaign_msb;
	auto uuid = diss(gen);
	uint8_t* uuid_ptr = reinterpret_cast<uint8_t*>(&uuid);
	memcpy(&campaign_msb, uuid_ptr, 8);
	memcpy(&campaign_lsb, uuid_ptr + 8, 8);
	campaign_lsb &= 0xffffffff00000000;

	for(size_t u = 0; u < generated_tuples_this_pass; u++)
	{
		generateTuple(&data[u], /**campaingOffset*/ randomNumbers[u%randomCnt], campaign_lsb, campaign_msb, /**eventID*/ u);
	}
}

TupleBufferPtr YSBFunctor::operator()()
{
	TupleBufferPtr buf = Dispatcher::instance().getBuffer();
	assert(buf->buffer != NULL);
	uint64_t generated_tuples_this_pass = buf->buffer_size / sizeof(ysbRecord);

	generate((ysbRecord*) buf->buffer, generated_tuples_this_pass, campaingCnt);

	buf->tuple_size_bytes = sizeof(ysbRecord);
	buf->num_tuples = generated_tuples_this_pass;
	return buf;
}

TupleBufferPtr YSBGeneratorSource::receiveData() {
    //we wait until the buffer is filled
	if(!preGenerated)
	{
		TupleBufferPtr buf = functor();
		generatedTuples += buf->num_tuples;
		generatedBuffers++;
		return buf;
	}
	else
	{
		TupleBufferPtr buf = Dispatcher::instance().getBuffer();
		buf = copyBuffer;
//		std::cout << "inbuffer=" << std::endl;
//		copyBuffer->print();
//		std::cout << "outbuffer=" << std::endl;
//		buf->print();
		generatedBuffers++;
		generatedTuples += buf->num_tuples;
		return buf;
	}
}

const std::string YSBGeneratorSource::toString() const{
    std::stringstream ss;
    ss << "YSBGeneratorSource(SCHEMA(" << schema.toString();
    ss << "), NUM_BUFFERS=" << num_buffers_to_process <<
    		" numberOfCampaings=" << numberOfCampaings << "))";
    return ss.str();
}
}
