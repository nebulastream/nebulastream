
#include <API/Query.hpp>
#include <API/Config.hpp>

namespace iotdb {

    class FogNode;
    typedef std::shared_ptr<FogNode> FogNodePtr;

    class FogTopologyPlan;
    typedef std::shared_ptr<FogTopologyPlan> FogTopologyPlanPtr;

    class Query;
    typedef std::shared_ptr<Query> QueryPtr;

    class QueryPlan;
    typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

    class QueryGraph;
    typedef std::shared_ptr<QueryGraph> QueryGraphPtr;
    class FogQueryExecutionGraph;
    typedef std::shared_ptr<FogQueryExecutionGraph> FogQueryExecutionGraphPtr;

    class FogTopologyManager;
    typedef std::shared_ptr<FogTopologyManager> FogTopologyManagerPtr;

    class FogManager;
    typedef std::shared_ptr<FogManager> FogManagerPtr;

    class FogMonitor;
    typedef std::shared_ptr<FogMonitor> FogMonitorPtr;

    class Optimizer;
    typedef std::shared_ptr<Optimizer> OptimizerPtr;


    class FogNodeProperties{
        /* compute power, memory capacity, network bandwidth and latency */
        /* network connection quality */
    };

    class FogNode{
    public:
        /** \brief stores the fog nodes this fog node receives its data from */
        std::vector<FogNodePtr> childs;
        /** \brief stores the fog nodes this fog node transmit data to */
        std::vector<std::weak_ptr<FogNode>> parents;
        /** \brief stores the query sub-graph processed on this node */
        QueryGraphPtr query_graph;
        uint64_t node_id;
    };

    class FogTopologyPlan{

        std::vector<FogNodePtr> source_nodes;
        std::vector<FogNodePtr> sink_nodes;
    };


    class FogTopologyManager{
    public:

        static FogTopologyPlanPtr getPlan(){
            return FogTopologyPlanPtr();
        }

        static FogQueryExecutionGraphPtr map(FogTopologyPlanPtr fog, QueryGraphPtr qg){
            return FogQueryExecutionGraphPtr();
        }

        static FogMonitorPtr deploy(FogQueryExecutionGraphPtr fqep){
            return FogMonitorPtr();
        }
    };

    class QueryGraph{
    public:
        static QueryGraphPtr create(){
            return QueryGraphPtr();
        }

        void add(QueryPlanPtr query_plan){

        }

        std::vector<OperatorPtr> source_nodes;
        std::vector<OperatorPtr> sink_nodes;
    };


    QueryPlanPtr getQueryPlan(Query& query){
        return QueryPlanPtr();
    }


    QueryGraphPtr optimizeQueryGraph(QueryGraphPtr query_graph){
        /** \todo Optimizer path to determine the operator and sub-query placement on individual fog nodes */
        /** \todo Optimizer path to determine the routing of data through the fog */

        return QueryGraphPtr();
    }

    class FogEvent{

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

        //FogRoutingPlan fog_rout_plan = FogTopologyManager::computeRoutingPlan(fog);


        DataSourcePtr source;

        Query&& query = std::move(Query::create(Config::create(), Schema::create(), source));

        QueryPlanPtr query_plan = getQueryPlan(query);

        QueryGraphPtr qg = QueryGraph::create();

        qg->add(query_plan);

        qg = optimizeQueryGraph(qg);

        FogQueryExecutionGraphPtr fqep = FogTopologyManager::map(fog, qg);

        FogMonitorPtr monitor = FogTopologyManager::deploy(fqep);

        while(true){

                if(!monitor->hasEvents()){
                    monitor->waitForEvents();
                    }

                handleEvents(monitor->getEvents());

        }


    }



}
