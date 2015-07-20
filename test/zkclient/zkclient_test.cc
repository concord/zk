#include <glog/logging.h>
#include <gtest/gtest.h>
#include <thread>
#include <zookeeper/zookeeper.h>
#include "bolt/zookeeper/ZKClient.hpp"
#include "bolt/testutils/ZooKeeperHarness.hpp"

using namespace bolt;

TEST_F(ZooKeeperHarness, CanNotSetNodeThatDoesNotExist) {
  auto data = folly::IOBuf::copyBuffer("thingo", 6);
  auto result = zk->setSync("/foobar", std::move(data));
  EXPECT_EQ(result.result, ZNONODE);
}

TEST_F(ZooKeeperHarness, CreateNodeThatDoesNotExist) {
  EXPECT_FALSE(zk->existsSync("/foobar").ok());
  auto data = folly::IOBuf::copyBuffer("thingo", 7);
  auto result = zk->createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE,
                               ZOO_EPHEMERAL);
  EXPECT_TRUE(result.ok());

  EXPECT_TRUE(zk->existsSync("/foobar").ok());
  auto nodeTuple = zk->getSync("/foobar");
  EXPECT_STREQ((char *)nodeTuple.data(), (char *)data->data());
}

TEST_F(ZooKeeperHarness, SetNodeThatDoesExist) {
  auto data = folly::IOBuf::copyBuffer("thingo", 7);
  zk->createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE,
                 ZOO_EPHEMERAL);
  auto data2 = folly::IOBuf::copyBuffer("asdf", 5);
  auto result = zk->setSync("/foobar", std::move(data2));
  EXPECT_TRUE(result.ok());
  auto nodeTuple = zk->getSync("/foobar");
  EXPECT_STREQ((char *)nodeTuple.data(), (char *)data2->data());
}

TEST_F(ZooKeeperHarness, createNode) {
  auto data = folly::IOBuf::copyBuffer("thingo", 7);
  zk->createSync("/foobar", std::move(data), &ZOO_OPEN_ACL_UNSAFE,
                 ZOO_EPHEMERAL);
  auto delResult = zk->delSync("/foobar");
  EXPECT_TRUE(delResult.ok());
}

int main(int argc, char **argv) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
