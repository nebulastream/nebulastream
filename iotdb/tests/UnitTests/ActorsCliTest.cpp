#include <gtest/gtest.h>
#include <Actors/CoordinatorActor.hpp>
#include <Actors/WorkerActor.hpp>

#include <Util/Logger.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include "caf/io/all.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

namespace NES {

class ActorsCliTest : public testing::Test {
  public:
    std::string host = "localhost";
    std::string queryString =
        "InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); ";

    static void SetUpTestCase() {
        NES::setupLogging("ActorsCliTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ActorCoordinatorWorkerTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down ActorsCli test class." << std::endl;
    }

    void TearDown() {
        QueryCatalog::instance().clearQueries();
    }
};

TEST_F(ActorsCliTest, DISABLED_testDeleteQuery) {
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;
    sleep(1);

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //TODO: we have to redesign the entire test
    string uuid;
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};
    scoped_actor self{system_coord};

    // check registration
    self->request(crd->getActorHandle(), task_timeout, register_query_atom::value,
                  queryString, "BottomUp").receive(
        [&uuid](const string& _uuid) mutable {
          uuid = _uuid;
          cout << "testDeleteQuery: got id=" << uuid << endl;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testDeleteQuery " << "\n" << error_msg);
        });
    NES_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    cout << "deploy_query_atom" << endl;
    bool successDeploy = false;
    self->request(crd->getActorHandle(), task_timeout, deploy_query_atom::value, uuid).receive(
        [&successDeploy](const bool& c) mutable {
          successDeploy = c;
          cout << "testDeleteQuery: deploy_query_atom success=" << c << endl;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during deploy_query_atom " << "\n" << error_msg);
        });
    EXPECT_TRUE(successDeploy);

    bool successDeregister = false;
    cout << "deregister_query_atom" << endl;
    self->request(crd->getActorHandle(), task_timeout, deregister_query_atom::value, uuid).receive(
        [&successDeregister](const bool& c) mutable {
          successDeregister = c;
          cout << "testDeleteQuery: deregister_query_atom success=" << c << endl;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during deregister_query_atom " << "\n" << error_msg);
        });
    EXPECT_TRUE(successDeregister);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop();
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);
}

}
