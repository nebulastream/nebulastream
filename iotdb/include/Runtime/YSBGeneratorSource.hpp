#ifndef INCLUDE_RUNTIME_YSBGENERATORSOURCE_HPP_
#define INCLUDE_RUNTIME_YSBGENERATORSOURCE_HPP_

#include <Core/TupleBuffer.hpp>

#include <boost/serialization/vector.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
//BOOST_SERIALIZATION_ASSUME_ABSTRACT(iotdb::DataSource)
//BOOST_CLASS_EXPORT_KEY(iotdb::YSBGeneratorSource)

namespace iotdb{

class YSBFunctor {
public:
	YSBFunctor(): campaingCnt(0){};

	YSBFunctor(size_t pCampaingCnt): campaingCnt(pCampaingCnt){};
	TupleBufferPtr operator()();

private:
   size_t campaingCnt;
};

class YSBGeneratorSource : public DataSource {
public:

	YSBGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process, size_t pCampaingCnt, bool preGenerated);



  TupleBufferPtr receiveData();
  const std::string toString() const;
  uint64_t numberOfCampaings;

private:
	YSBGeneratorSource();
	friend class boost::serialization::access;
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version)
	{
		ar & boost::serialization::base_object<DataSource>(*this);
		ar & numberOfCampaings;
	}

  iotdb::YSBFunctor functor;
  bool preGenerated;
  TupleBufferPtr copyBuffer;
};

}
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::YSBGeneratorSource)

#endif /* INCLUDE_RUNTIME_YSBGENERATORSOURCE_HPP_ */
