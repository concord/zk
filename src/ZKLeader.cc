#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>
#include "bolt/zookeeper/ZKLeader.hpp"
#include "bolt/utils/url_utils.hpp"
#include "bolt/utils/string_utils.hpp"

namespace bolt {
const folly::Uri &ZKLeader::uri() const { return zkUri_; }

std::shared_ptr<ZKClient> ZKLeader::client() const { return zk_; }


ZKLeader::ZKLeader(folly::Uri zkUri,
                   std::function<void(ZKLeader *)> leaderfn,
                   std::function<void(int, int, std::string, ZKClient *)> zkcb)
  : zkUri_(zkUri), leadercb_(leaderfn), zkcb_(zkcb) {
  using namespace std::placeholders;
  auto cb = std::bind(&ZKLeader::zkCbWrapper, this, _1, _2, _3, _4);
  const auto baseElectionPath = zkUri_.path().toStdString() + "/election";
  const auto baseElectionId = baseElectionPath + "/" + uuid() + "_n_";
  zk_ = std::make_shared<ZKClient>(cb, zookeeperHostsFromUrl(zkUri_), 500, 0,
                                   true /*yield until connected*/);
  LOG(INFO) << "Watching: " << baseElectionPath;
  auto zkret = zk_->existsSync(baseElectionPath, true);

  if(zkret.result == ZNONODE) {
    touchZKPathSync(baseElectionPath);
  } else {
    CHECK(zkret.result == ZOK)
      << "Failed to watch the directory ret code: " << zkret.result;
  }

  LOG(INFO) << "Creating election node: " << baseElectionId;
  zkret = zk_->createSync(baseElectionId, std::make_unique<IOBuf>(),
                          &ZOO_OPEN_ACL_UNSAFE, ZOO_SEQUENCE | ZOO_EPHEMERAL);
  CHECK(zkret.result == ZOK)
    << "Couldn't create election path: " << baseElectionId
    << ", ret: " << zkret.result;
  std::string ephemeral((char *)zkret.data(), zkret.buff->length());
  LOG(INFO) << "Ephemeral node: " << ephemeral;
  electionPath_ = std::move(ephemeral);
  auto optId = extractIdFromEphemeralPath(electionPath_);
  CHECK(optId) << "Could not parse id from ephemeral path";
  id_ = optId.get();
  LOG(INFO) << "Leader election id: " << id_;
  leaderElect(0, 0, "");
}

void ZKLeader::touchZKPathSync(const std::string &nestedPath) {
  std::vector<std::string> paths;
  boost::split(paths, nestedPath, boost::is_any_of("/"));
  CHECK(!paths.empty()) << "Empty nested path: " << nestedPath;
  std::string path = "";

  for(auto p : paths) {
    if(p.empty() || p == "/") {
      continue;
    }

    path += "/" + p;
    auto zkret = zk_->existsSync(path);

    if(zkret.result == ZNONODE) {
      LOG(INFO) << "Creating directory: " << path;
      zkret = zk_->createSync(path, std::make_unique<IOBuf>(),
                              &ZOO_OPEN_ACL_UNSAFE, 0);
      CHECK(zkret.result == ZOK)
        << "failed to create path, code: " << zkret.result;
    } else {
      CHECK(zkret.result == ZOK)
        << "failed to read path, code: " << zkret.result;
    }
  }
}

void ZKLeader::leaderElect(int type, int state, std::string path) {

  const std::string baseElectionPath =
    zkUri_.path().toStdString() + "/election";

  auto zkret = zk_->childrenSync(baseElectionPath, true);
  auto retcode = zkret.result;

  if(retcode == -1 || retcode == ZCLOSING || retcode == ZSESSIONEXPIRED) {
    LOG(ERROR) << "Zookeeper not ready|closing|expired socket [MYID: " << id_
               << "] ";
    return;
  }

  if(!(retcode == ZOK || retcode == ZNONODE)) {
    LOG(ERROR) << "[MYID " << id_ << "] "
               << "No children: " << baseElectionPath
               << ", retcode: " << zkret.result;
    return;
  }

  std::set<int32_t> runningLeaders;

  for(auto &s : zkret.strings) {
    LOG(INFO) << "Running for election: [MYID: " << id_ << "] " << s
              << ", base path: " << path;
    runningLeaders.insert(extractIdFromEphemeralPath(s).get());
  }

  if(!runningLeaders.empty()) {
    if(id_ >= 0) {
      if(runningLeaders.find(id_) == runningLeaders.end()) {
        CHECK(!isLeader_)
          << "Cannot be leader and then not leader. Doesn't work. exit";
        isLeader_ = false;
        LOG(ERROR) << "Could not find my id [MYID: " << id_
                   << "]. out of sync w/ zookeeper";
      } else if(id_ <= *runningLeaders.begin()) {
        LOG(INFO) << "LEADER! - ONE ID TO RULE THEM ALL: " << id_;
        if(!isLeader_) {
          // ONLY CALL ONCE
          leadercb_(this);
        }
        isLeader_ = true;
      }
    }
  }
}

void ZKLeader::zkCbWrapper(int type,
                           int state,
                           std::string path,
                           ZKClient *cli) {
  LOG(INFO) << "Notification received[MYID: " << id_
            << "]. type: " << ZKClient::printZookeeperEventType(type)
            << ", state: " << ZKClient::printZookeeperState(state)
            << ", path: " << path;

  if(id_ > 0) {
    leaderElect(type, state, path);
  }
  zkcb_(type, state, path, cli);
}
bool ZKLeader::isLeader() const { return isLeader_; }
int32_t ZKLeader::id() const { return id_; }
std::string ZKLeader::ephemeralPath() const { return electionPath_; }
boost::optional<int32_t>
ZKLeader::extractIdFromEphemeralPath(const std::string &path) {
  // zookeeper ephemeral ids are always guaranteed to end in 10
  // monotonically increasing digits by the C api - see zookeeper.h
  static const boost::regex zkid(".*(\\d{10})$");
  boost::smatch what;

  if(boost::regex_search(path, what, zkid)) {
    return std::stoi(what[1].str());
  }

  return boost::none;
}
}
