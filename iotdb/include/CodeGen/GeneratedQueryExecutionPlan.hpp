/*
 * HandCodedQueryExecutionPlan.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_
#define INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_

#include <CodeGen/QueryExecutionPlan.hpp>
#include <boost/serialization/export.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <stdint.h>
#include <API/InputQuery.hpp>
// class TupleBuffer;

namespace iotdb {

class GeneratedQueryExecutionPlan : public QueryExecutionPlan {
public:
    GeneratedQueryExecutionPlan();
    GeneratedQueryExecutionPlan(InputQuery* query, PipelineStagePtr* ptr);

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf){
        return 0;
    };
protected:
    InputQuery* query;
    PipelineStagePtr* ptr;
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
BOOST_CLASS_EXPORT_KEY(iotdb::GeneratedQueryExecutionPlan)
#endif /* INCLUDE_GENERATEDQUERYEXECUTIONPLAN_H_ */
