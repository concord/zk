#pragma once
#include <memory>
#include <boost/optional.hpp>
#include <atomic>
#include <folly/Uri.h>
#include "bolt/zookeeper/ZKClient.hpp"
namespace bolt {
class ZKLeader {
  public:
  // utility functions
  static boost::optional<int32_t>
  extractIdFromEphemeralPath(const std::string &path);

  // Note that this is a very simple leader election.
  // it is prone to the herd effect. Since our scheduler list will be small
  // this is considered OK behavior.
  ZKLeader(folly::Uri zkUri,
           std::function<void(ZKLeader *)> leaderfn,
           std::function<void(int, int, std::string, ZKClient *)> zkcb);
  bool isLeader() const;
  int32_t id() const;
  std::string ephemeralPath() const;
  const folly::Uri &uri() const;
  std::shared_ptr<ZKClient> client() const;

  private:
  void zkCbWrapper(int type, int state, std::string path, ZKClient *);
  void touchZKPathSync(const std::string &path);
  void leaderElect(int type, int state, std::string path);
  folly::Uri zkUri_;
  std::function<void(ZKLeader *)> leadercb_;
  std::function<void(int, int, std::string, ZKClient *)> zkcb_;
  std::atomic<bool> isLeader_{false};
  int32_t id_{-1};
  std::shared_ptr<ZKClient> zk_;
  std::string electionPath_;
};
}
