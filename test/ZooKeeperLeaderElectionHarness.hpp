#pragma once
#include <chrono>
#include <deque>
#include <folly/Uri.h>
#include "bolt/testutils/SubprocessHarness.hpp"
#include "bolt/zookeeper/ZKLeader.hpp"

namespace bolt {
static const std::vector<std::string> kZooEnv{
  "ZOO_LOG4J_PROP=INFO,ROLLINGFILE",
  "CLASSPATH=/usr/share/java/jline.jar:/usr/share/java/log4j-1.2.jar:"
  "/usr/share/java/xercesImpl.jar:/usr/share/java/xmlParserAPIs.jar:"
  "/usr/share/java/netty.jar:/usr/share/java/slf4j-api.jar:"
  "/usr/share/java/slf4j-log4j12.jar:/usr/share/java/zookeeper.jar",
  "ZOOMAIN=org.apache.zookeeper.server.quorum.QuorumPeerMain"};
static const std::string kZooCmd =
  "java \"-Dzookeeper.log.dir=${ZOO_LOG_DIR} "
  " -Dzookeeper.root.logger=${ZOO_LOG4J_PROP}\" "
  " -cp ${CLASSPATH} ${ZOOMAIN} ${ZOOCFG}";
class ZooKeeperLeaderElectionHarness : public SubprocessHarness {
  public:
  ZooKeeperLeaderElectionHarness() : SubprocessHarness(kZooCmd, kZooEnv) {
    env_.push_back("ZOOCFG=" + tmpDir_ + "/zoo.cfg");
    env_.push_back("ZOO_LOG_DIR=" + tmpDir_ + "/");
    writeConfigFile();
  }

  virtual void SetUp() {
    SubprocessHarness::SetUp();

    for(auto i = 0u; i < 3; ++i) {
      leaders.push_back(std::make_shared<ZKLeader>(
        zkUri, [](ZKLeader *) { LOG(INFO) << "harness leader cb"; },
        [](int type, int state, std::string path, ZKClient *cli) {
          LOG(INFO) << "harness zoo cb";
        }));
    }

    bool haveLeader = false;
    int maxTries = 100;

    while(!haveLeader) {
      for(auto &ptr : leaders) {
        if(ptr->isLeader()) {
          haveLeader = true;
          break;
        }
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      if(maxTries-- < 0) {
        break;
      }
    }

    CHECK(haveLeader) << "Could not get a single leader elected. FIXME NOW";
  }
  virtual void TearDown() {
    leaders.clear();
    SubprocessHarness::TearDown();
  }

  virtual void writeConfigFile() {
    std::ofstream cfg(tmpDir_ + "/zoo.cfg");
    // Documentation:  http://goo.gl/m1n2jN
    // tickTIme is in millisecs. this is for testing
    //
    const std::string config("tickTime=3\n"
                             "initLimit=10\n"
                             "syncLimit=5\n"
                             "traceFile=" + tmpDir_ + "/tracefile.log\n"
                                                      "dataDir=" + tmpDir_
                             + "/data\n"
                               "clientPort=2181\n");
    cfg.write(config.c_str(), config.size());
    cfg.close();
  }
  folly::Uri zkUri{"zk:///bolt?host=localhost:2181"};
  std::deque<std::shared_ptr<ZKLeader>> leaders;
};
}
