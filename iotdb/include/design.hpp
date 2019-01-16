
#include <API/Config.hpp>
#include <algorithm>

#include "API/InputQuery.hpp"
namespace iotdb {

/** Client Side **/
    class InputQuery;//API
    typedef std::shared_ptr<InputQuery> InputQueryPtr;

    class Schema;//API
    typedef std::shared_ptr<Schema> SchemaPtr;

    class Stream;//API
    typedef std::shared_ptr<Stream> StreamPtr;

/** Master Node -- Fog Components **/    
    class FogToplogyManager;
    typedef std::shared_ptr<FogToplogyManager> FogToplogyManagerPtr;

    class FogTopologyPlan;
    typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;
    
    class FogToplogyNode;
    typedef std::shared_ptr<FogToplogyNode> FogToplogyNodePtr;

    class FogToplogyLink;
    typedef std::shared_ptr<FogToplogyLink> FogToplogyLinkPtr;

    class FogNodeProperties;
    typedef std::shared_ptr<FogNodeProperties> FogNodePropertiesPtr;


/** Master Node -- Query Components **/    
    class LogicalQueryManager;
    typedef std::shared_ptr<LogicalQueryManager> LogicalQueryManagerPtr;

  /** for deploying multiple queries at once */
    class LogicalQueryGraph;
    typedef std::shared_ptr<LogicalQueryGraph> LogicalQueryGraphPtr;

    class LogicalQueryPlan;
    typedef std::shared_ptr<LogicalQueryPlan> LogicalQueryPlanPtr;

/** Master Node -- Optimizer Components **/    
    class FogOptimizer;
    typedef std::shared_ptr<FogOptimizer> FogOptimizerPtr;

    class FogExecutionPlan;
    typedef std::shared_ptr<FogExecutionPlan> FogExecutionPlanPtr;

    class FogRuntime;
    typedef std::shared_ptr<FogRuntime> FogRuntimePtr;

/** Master Node -- Monitoring Components **/    
    class FogMonitor;
    typedef std::shared_ptr<FogMonitor> FogMonitorPtr;

/** FogNode Device **/    
    class FogNodeManager;
    typedef std::shared_ptr<FogNodeManager> FogNodeManagerPtr;
    
    class FogExecutionEngine;
    typedef std::shared_ptr<FogExecutionEngine> FogExecutionEnginePtr;
    
    class FogSensorEngine;
    typedef std::shared_ptr<FogSensorEngine> FogSensorEnginePtr;
    
 /* ----------- IMPL ---------*/  
    
    class FogToplogyNode{
    public:
        /** \brief stores the fog nodes this fog node receives its data from */
        std::vector<FogToplogyNodePtr> childs;
        /** \brief stores the fog nodes this fog node transmit data to */
        std::vector<std::weak_ptr<FogToplogyNode>> parents;
        /** \brief stores the query sub-graph processed on this node */
        LogicalQueryGraphPtr query_graph;
        FogNodePropertiesPtr properties;
        uint64_t node_id;
    };

    class FogTopologyPlan{
        // how to model Links?
        std::vector<FogToplogyNodePtr> source_nodes;
        std::vector<FogToplogyNodePtr> sink_nodes;
    };

    class FogTopologyManager{
        public:

            static FogTopologyPlanPtr getPlan(){
                return FogTopologyPlanPtr();
            }
    };

    class FogOptimizer
    {
    public:
    	 static FogExecutionPlanPtr map(FogTopologyPlanPtr fog, LogicalQueryGraphPtr qg){
    		 return FogExecutionPlanPtr();
    	 }

    	  LogicalQueryGraphPtr optimizeQueryGraph(LogicalQueryGraphPtr query_graph){
    	        /** \todo Optimizer path to determine the operator and sub-query placement on individual fog nodes */
    	        /** \todo Optimizer path to determine the routing of data through the fog */

    	        return LogicalQueryGraphPtr();
    	    }


    };

    class FogRuntime
    {
    public:
    	static FogMonitorPtr deploy(FogExecutionPlanPtr fqep){
			return FogMonitorPtr();
		}
    };


    class LogicalQueryGraph{
        public:
            static LogicalQueryGraphPtr create(){
                return LogicalQueryGraphPtr();
            }

            void add(LogicalQueryPlanPtr query_plan){

            }

            std::vector<OperatorPtr> source_nodes;
            std::vector<OperatorPtr> sink_nodes;
    };

    class LogicalQueryManager
    {
    public:
    	LogicalQueryGraphPtr getQueryGraph(){
    		return logQueryGraph;
		}

    	LogicalQueryPlanPtr createLogQueryPlan(InputQuery& query){
            return LogicalQueryPlanPtr();
        }

        void addQueryToGraph(LogicalQueryPlanPtr plan)
        {
        }



        LogicalQueryGraphPtr logQueryGraph;
    };


    class FogEvent{
        //what to do with this?
    };

    class FogMonitor{
    public:
        bool hasEvents(){
            return false;
        }
        const std::vector<FogEvent> getEvents(){
            return std::vector<FogEvent>();
        }
        void waitForEvents(){

        }
    };
    void handleEvents(const std::vector<FogEvent>& events){

    }


    void test_design(){


        FogTopologyPlanPtr fog = FogTopologyManager::getPlan();

        DataSourcePtr source;

        InputQuery&& iQuery = std::move(InputQuery::create(Config::create(), Schema::create(), source));

        LogicalQueryManager logQueryMgn;

        LogicalQueryPlanPtr logPlan = logQueryMgn.createLogQueryPlan(iQuery);

        logQueryMgn.addQueryToGraph(logPlan);

        FogExecutionPlanPtr fep = FogOptimizer::map(fog, logQueryMgn.getQueryGraph());

        FogMonitorPtr monitor = FogRuntime::deploy(fep);

        while(true){

                if(!monitor->hasEvents()){
                    monitor->waitForEvents();
                    }

                handleEvents(monitor->getEvents());

        }


    }



}
