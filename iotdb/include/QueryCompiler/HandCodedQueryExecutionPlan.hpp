/*
 * HandCodedQueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_

#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <stdint.h>
// class TupleBuffer;

namespace iotdb {

class HandCodedQueryExecutionPlan : public QueryExecutionPlan {
public:
  HandCodedQueryExecutionPlan();
  virtual ~HandCodedQueryExecutionPlan();
  virtual bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) = 0;
private:
  friend class boost::serialization::access;

  template<class Archive>
  void serialize(Archive & ar, const unsigned int version)
  {
      ar & boost::serialization::base_object<QueryExecutionPlan>(*this);
  }
};
}
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::HandCodedQueryExecutionPlan)
#endif /* INCLUDE_HANDCODEDQUERYEXECUTIONPLAN_H_ */
