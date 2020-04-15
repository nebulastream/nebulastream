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

    static void TearDownTestCase() {
        std::cout << "Tear down ActorsCli test class." << std::endl;
    }

    void TearDown() {
        QueryCatalog::instance().clearQueries();
    }
};

TEST_F(ActorsCliTest, testDeleteQuery) {
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

    // check registration
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
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testDeleteQuery " << "\n" << error_msg);
        });
    NES_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    cout << "deploy_query_atom" << endl;
    bool success = false;
    self->request(crd->getActorHandle(), task_timeout, deploy_query_atom::value, uuid).receive(
        [&success](const bool& c) mutable {
          success = c;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during deploy_query_atom " << "\n" << error_msg);
        });
    EXPECT_TRUE(success);

    bool success2 = false;
    cout << "deregister_query_atom" << endl;
    self->request(crd->getActorHandle(), task_timeout, deregister_query_atom::value, uuid).receive(
        [&success2](const bool& c) mutable {
          success2 = c;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during deregister_query_atom " << "\n" << error_msg);
        });
    EXPECT_TRUE(success2);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop();
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);
}

TEST_F(ActorsCliTest, testSequentialMultiQueries) {
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

    // check registration
    string uuid;
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};
    scoped_actor self{system_coord};

    for (int i = 0; i < 1; i++) {
        NES_INFO("ACTORSCLITEST (testSequentialMultiQueries): Sequence " << i);

        string uuid;
        self->request(crd->getActorHandle(), task_timeout, register_query_atom::value,
                      queryString, "BottomUp").receive(
            [&uuid](const string& _uuid) mutable {
              uuid = _uuid;
            }, [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST (testSequentialMultiQueries): Error during testShowRegistered " << "\n"
                                                                                                 << error_msg);
            });
        NES_INFO(
            "ACTORSCLITEST (testSequentialMultiQueries): Registration completed with query ID " << uuid);
        EXPECT_TRUE(!uuid.empty());

        bool success = false;
        self->request(crd->getActorHandle(), task_timeout, deploy_query_atom::value, uuid).receive(
            [&success](const bool& c) mutable {
              success = c;
            }, [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST: Error during deploy_query_atom " << "\n" << error_msg);
            });
        EXPECT_TRUE(success);

        bool success2 = false;
        cout << "deregister_query_atom" << endl;
        self->request(crd->getActorHandle(), task_timeout, deregister_query_atom::value, uuid).receive(
            [&success2](const bool& c) mutable {
              success2 = c;
            }, [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST: Error during deregister_query_atom " << "\n" << error_msg);
            });
        EXPECT_TRUE(success2);
    }

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop();
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator();
    EXPECT_TRUE(retStopCord);
}
}
