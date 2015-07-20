#include <bolt/glog_init.hpp>
#include <algorithm>
#include <limits>
#include <gtest/gtest.h>
#include <zookeeper/zookeeper.h>
#include "bolt/testutils/ZooKeeperLeaderElectionHarness.hpp"
#include "bolt/utils/Random.hpp"

using namespace bolt;

TEST_F(ZooKeeperLeaderElectionHarness, ctor) {
  // pop the leader (first one)
  for(;;) {
    leaders.pop_front(); // allow for zk conn to close
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    if(leaders.empty()) {
      break;
    }

    LOG(INFO) << "Leaders ID's left: "
              << std::accumulate(
                   leaders.begin(), leaders.end(), std::string(),
                   [](const std::string &a, std::shared_ptr<ZKLeader> b) {
                     auto bstr = std::to_string(b->id());
                     return (a.empty() ? bstr : a + "," + bstr);
                   });


    LOG(INFO) << "Leaders left: " << leaders.size();
    bool haveLeader = false;
    int maxTries = 100;

    while(!haveLeader) {
      for(auto &ptr : leaders) {
        if(ptr->isLeader()) {
          LOG(INFO) << "Found leader: " << ptr->id();
          haveLeader = true;
          break;
        }
      }

      std::this_thread::yield();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      if(maxTries-- < 0) {
        break;
      }
    }

    EXPECT_EQ(true, haveLeader);
  }
}


TEST_F(ZooKeeperLeaderElectionHarness, randomPopingOrder) {
  int maxNumberOfAdditions = 20;
  const auto kHalfOfLuck = std::numeric_limits<uint64_t>::max() / 2;
  Random rand;
  for(;;) {
    if(rand.nextRand() > kHalfOfLuck) {
      leaders.pop_front();
    } else {
      leaders.pop_back();
    }
    // allow for zk conn to close
    std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    if(maxNumberOfAdditions-- > 0) {
      leaders.push_back(std::make_shared<ZKLeader>(
        zkUri, [](ZKLeader *) { LOG(INFO) << "testbody leader cb"; },
        [](int type, int state, std::string path, ZKClient *cli) {
          LOG(INFO) << "testbody zoo cb";
        }));
      std::this_thread::yield();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if(leaders.empty()) {
      break;
    }

    LOG(INFO) << "Leaders ID's left: "
              << std::accumulate(
                   leaders.begin(), leaders.end(), std::string(),
                   [](const std::string &a, std::shared_ptr<ZKLeader> b) {
                     auto bstr = std::to_string(b->id());
                     return (a.empty() ? bstr : a + "," + bstr);
                   });


    LOG(INFO) << "Leaders left: " << leaders.size();
    bool haveLeader = false;
    int maxTries = 100;

    while(!haveLeader) {
      for(auto &ptr : leaders) {
        if(ptr->isLeader()) {
          LOG(INFO) << "Found leader: " << ptr->id();
          haveLeader = true;
          break;
        }
      }

      std::this_thread::yield();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      if(maxTries-- < 0) {
        break;
      }
    }

    EXPECT_EQ(true, haveLeader);
  }
}

TEST(ZookeeperLeaderEphemeralNode, id_parsing) {
  auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_n_0000000002";
  auto ret = ZKLeader::extractIdFromEphemeralPath(str);
  ASSERT_EQ(2, ret.get());
}

TEST(ZookeeperLeaderEphemeralNode, id_parsing_bad_id) {
  auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_asdfasdf";
  auto ret = ZKLeader::extractIdFromEphemeralPath(str);
  ASSERT_EQ(boost::none, ret);
}

TEST(ZookeeperLeaderEphemeralNode, id_parsing_close_but_no_cigar) {
  // match hast to be exactly 10 digits as specified by zk api
  auto str = "asdfasdfasdf_70f7d1ad-6a4c-4ad4-b187-d33483ebd728_n_000000002";
  auto ret = ZKLeader::extractIdFromEphemeralPath(str);
  ASSERT_EQ(boost::none, ret);
}

int main(int argc, char **argv) {
  bolt::logging::glog_init(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
