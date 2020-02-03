#include <gtest/gtest.h>
#include <Actors/CoordinatorActor.hpp>
#include <Actors/WorkerActor.hpp>

#include <Util/Logger.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include "caf/io/all.hpp"
#include <Catalogs/PhysicalStreamConfig.hpp>

namespace NES {

class ActorsCliTest : public testing::Test {
  public:
    std::string host = "localhost";
    std::string queryString =
        "InputQuery::from(default_logical).filter(default_logical[\"id\"] > 42).print(std::cout); ";

    static void SetUpTestCase() {
        setupLogging();
        NES_INFO("Setup ActorCoordinatorWorkerTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ActorsCli test class." << std::endl;
    }

    void TearDown() {
        QueryCatalog::instance().clearQueries();
    }

  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NESLogger->setLevel(log4cxx::Level::getDebug());

        // add appenders and other will inherit the settings
        NESLogger->addAppender(file);
        NESLogger->addAppender(console);
    }
};

TEST_F(ActorsCliTest, testRegisterUnregisterSensor) {
    NES_INFO("ACTORSCLITEST: Running test testRegisterUnregisterSensor");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testRegisterUnregisterSensor):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testRegisterUnregisterSensor): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testRegisterUnregisterSensor): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

    //Prepare Actor System
    scoped_actor self{system_coord};
    bool connected = false;
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testRegisterUnregisterSensor " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    self->request(worker, task_timeout, disconnect_atom::value);
    //TODO: this should also test the result
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host, c_cfg.publish_port);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testSpawnDespawnCoordinatorWorkers) {

    NES_INFO("ACTORSCLITEST: Running test testSpawnDespawnCoordinatorWorkers");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testSpawnDespawnCoordinatorWorkers):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testSpawnDespawnCoordinatorWorkers): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testSpawnDespawnCoordinatorWorkers): coordinator successfully published at port "
            << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

    //Prepare Actor System
    scoped_actor self{system_coord};
    bool connected = false;
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port)
        .receive(
            [&connected](const bool& c) mutable {
              connected = c;
              std::this_thread::sleep_for(std::chrono::seconds(1));
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST: Error during testSpawnDespawnCoordinatorWorkers " << "\n" << error_msg);
            });
    EXPECT_TRUE(connected);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowTopology) {

    NES_INFO("ACTORSCLITEST: Running test testShowTopology");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testShowTopology):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testShowTopology): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testShowTopology): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

    //Prepare Actor System
    scoped_actor self{system_coord};
    bool connected = false;
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testShowTopology " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    self->request(coordinator, task_timeout, topology_json_atom::value);
    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowRegistered) {

    NES_INFO("ACTORSCLITEST: Running test testShowRegistered");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testShowRegistered):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testShowRegistered): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testShowRegistered): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

    //Prepare Actor System
    scoped_actor self{system_coord};
    bool connected = false;
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    // check registration
    string uuid;
    self->request(coordinator, task_timeout, register_query_atom::value, queryString, "BottomUp")
        .receive([&uuid](const string& _uuid) mutable {
          uuid = _uuid;
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
        });

    NES_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    // check length of registered queries
    size_t query_size = 0;
    self->request(coordinator, task_timeout, show_registered_queries_atom::value)
        .receive(
            [&query_size](const size_t length) mutable {
              query_size = length;
              NES_INFO("ACTORSCLITEST: Query length " << length);
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST: Error during testShowRegistered " << "\n" << error_msg);
            });
    EXPECT_EQ(query_size, 1);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testDeleteQuery) {

    NES_INFO("ACTORSCLITEST: Running test testDeleteQuery");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testDeleteQuery):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testDeleteQuery): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testDeleteQuery): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port,
                                             w_cfg.receive_port);

    //Prepare Actor System
    scoped_actor self{system_coord};
    bool connected = false;
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testDeleteQuery " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    // check registration
    string uuid;
    self->request(coordinator, task_timeout, register_query_atom::value,
                  queryString, "BottomUp").receive(
        [&uuid](const string& _uuid) mutable {
          uuid = _uuid;
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testDeleteQuery " << "\n" << error_msg);
        });
    NES_INFO("ACTORSCLITEST: Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    // check length of registered queries
    size_t query_size = 0;
    self->request(coordinator, task_timeout, show_registered_queries_atom::value)
        .receive(
            [&query_size](const size_t length) mutable {
              query_size = length;
              NES_INFO("ACTORSCLITEST: Query length " << length);
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST: Error during testDeleteQuery " << "\n" << error_msg);
            });
    EXPECT_EQ(query_size, 1);

    self->request(coordinator, task_timeout, deploy_query_atom::value, uuid);
    self->request(coordinator, task_timeout, deregister_query_atom::value, uuid);
    self->request(coordinator, task_timeout, show_running_queries_atom::value);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowRunning) {
    NES_INFO("ACTORSCLITEST: Running test testShowRunning");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testShowRunning):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testShowRunning): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testShowRunning): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port);

    bool connected = false;
    scoped_actor self{system_coord};
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST (testShowRunning): Error during testShowRunning " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    string uuid;
    self->request(coordinator, task_timeout, register_query_atom::value,
                  queryString, "BottomUp").receive(
        [&uuid](const string& _uuid) mutable {
          uuid = _uuid;
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST (testShowRunning): Error during testShowRunning " << "\n" << error_msg);
        });
    NES_INFO("ACTORSCLITEST (testShowRunning): Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    self->request(coordinator, task_timeout, deploy_query_atom::value, uuid);
    self->request(coordinator, task_timeout, show_running_queries_atom::value);
    self->request(coordinator, task_timeout, deregister_query_atom::value, uuid);
    self->request(coordinator, task_timeout, show_running_queries_atom::value);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testShowOperators) {
    NES_INFO("ACTORSCLITEST: Running test testShowOperators");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};

    auto coordinator = system_coord.spawn<NES::CoordinatorActor>();
    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testShowOperators):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordinator, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testShowOperators): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testShowOperators): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto worker = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port);

    bool connected = false;
    scoped_actor self{system_coord};
    self->request(worker, task_timeout, connect_atom::value, w_cfg.host,
                  c_cfg.publish_port).receive(
        [&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST (testShowOperators): Error during testShowOperators " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    string uuid;
    self->request(coordinator, task_timeout, register_query_atom::value,
                  queryString, "BottomUp").receive(
        [&uuid](const string& _uuid) mutable {
          uuid = _uuid;
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST (testShowOperators): Error during testShowOperators " << "\n" << error_msg);
        });
    NES_INFO("ACTORSCLITEST (testShowOperators): Registration completed with query ID " << uuid);
    EXPECT_TRUE(!uuid.empty());

    self->request(coordinator, task_timeout, deploy_query_atom::value, uuid);
    self->request(coordinator, task_timeout, show_running_operators_atom::value);
    self->request(coordinator, task_timeout, deregister_query_atom::value, uuid);
    self->request(coordinator, task_timeout, show_running_queries_atom::value);

    self->request(worker, task_timeout, exit_reason::user_shutdown);
    self->request(coordinator, task_timeout, exit_reason::user_shutdown);
}

TEST_F(ActorsCliTest, testSequentialMultiQueries) {

    NES_INFO("ACTORSCLITEST: Running test testSequentialMultiQueries");
    CoordinatorActorConfig c_cfg;
    c_cfg.load<io::middleman>();
    actor_system system_coord{c_cfg};
    auto coordHandler = system_coord.spawn<NES::CoordinatorActor>();

    // try to publish actor at given port
    NES_INFO("ACTORSCLITEST (testSequentialMultiQueries):  try publish at port" << c_cfg.publish_port);
    auto expected_port = io::publish(coordHandler, c_cfg.publish_port);
    if (!expected_port) {
        NES_ERROR("ACTORSCLITEST (testSequentialMultiQueries): publish failed: "
                      << system_coord.render(expected_port.error()));
        return;
    }
    NES_INFO(
        "ACTORSCLITEST (testSequentialMultiQueries): coordinator successfully published at port " << *expected_port);

    WorkerActorConfig w_cfg;
    w_cfg.load<io::middleman>();
    actor_system sw{w_cfg};
    PhysicalStreamConfig streamConf;
    auto workerHandler = sw.spawn<NES::WorkerActor>(w_cfg.ip, w_cfg.publish_port, w_cfg.receive_port);

    bool connected = false;
    scoped_actor self{system_coord};
    self->request(workerHandler, task_timeout, connect_atom::value, w_cfg.host, c_cfg.publish_port)
        .receive([&connected](const bool& c) mutable {
          connected = c;
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }, [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORSCLITEST: Error during testSequentialMultiQueries " << "\n" << error_msg);
        });
    EXPECT_TRUE(connected);

    for (int i = 0; i < 1; i++) {
        NES_INFO("ACTORSCLITEST (testSequentialMultiQueries): Sequence " << i);

        string uuid;
        self->request(coordHandler, task_timeout, register_query_atom::value,
                      queryString, "BottomUp").receive(
            [&uuid](const string& _uuid) mutable {
              uuid = _uuid;
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "ACTORSCLITEST (testSequentialMultiQueries): Error during testShowRegistered " << "\n" << error_msg);
            });
        NES_INFO("ACTORSCLITEST (testSequentialMultiQueries): Registration completed with query ID " << uuid);
        EXPECT_TRUE(!uuid.empty());

        self->request(coordHandler, task_timeout, deploy_query_atom::value, uuid);
        self->request(coordHandler, task_timeout, show_running_operators_atom::value);
        self->request(coordHandler, task_timeout, deregister_query_atom::value, uuid);
        self->request(coordHandler, task_timeout, show_running_operators_atom::value);
    }

    self->request(workerHandler, task_timeout, exit_reason::user_shutdown);
    self->request(coordHandler, task_timeout, exit_reason::user_shutdown);
}
}
