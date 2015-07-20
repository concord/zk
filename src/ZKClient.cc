#include "bolt/zookeeper/ZKClient.hpp"

namespace bolt {
using namespace ::folly;

static const int kMaxTriesPerSyncOperation = 10;

static void
watchCb(zhandle_t *zh, int type, int state, const char *path, void *ctx);

static void statCompletionCb(int rc, const struct Stat *stat, const void *data);

static void stringCompletionCb(int rc, const char *value, const void *data);
static void stringsAndStatCompletionCb(int rc,
                                       const struct String_vector *strs,
                                       const struct Stat *stat,
                                       const void *data);
static void voidCompletionCb(int rc, const void *data);

static void dataCompletionCb(int rc,
                             const char *value,
                             int value_len,
                             const struct Stat *stat,
                             const void *data);

static std::shared_ptr<Promise<ZKResult>> promiseFromData(const void *data) {
  const Promise<ZKResult> *constPromise =
    static_cast<const Promise<ZKResult> *>(data);

  Promise<ZKResult> *promise = const_cast<Promise<ZKResult> *>(constPromise);

  return std::shared_ptr<Promise<ZKResult>>(promise);
}

// copy from stout / modified w/ __builtin_unreachable()
bool ZKClient::retryable(int rc) {
  switch(rc) {
  case ZCONNECTIONLOSS:
  case ZOPERATIONTIMEOUT:
  case ZSESSIONEXPIRED:
  case ZSESSIONMOVED:
    return true;

  case ZOK: // No need to retry!

  case ZSYSTEMERROR: // Should not be encountered, here for completeness.
  case ZRUNTIMEINCONSISTENCY:
  case ZDATAINCONSISTENCY:
  case ZMARSHALLINGERROR:
  case ZUNIMPLEMENTED:
  case ZBADARGUMENTS:
  case ZINVALIDSTATE:

  case ZAPIERROR: // Should not be encountered, here for completeness.
  case ZNONODE:
  case ZNOAUTH:
  case ZBADVERSION:
  case ZNOCHILDRENFOREPHEMERALS:
  case ZNODEEXISTS:
  case ZNOTEMPTY:
  case ZINVALIDCALLBACK:
  case ZINVALIDACL:
  case ZAUTHFAILED:
  case ZCLOSING:
  case ZNOTHING: // Is this used? It's not exposed in the Java API.
    return false;

  default:
    LOG(FATAL) << "Unknown ZooKeeper code: " << rc;
    __builtin_unreachable(); // Make compiler happy.
  }
}


void ZKClient::init(bool block) {
  DLOG(INFO) << "Initializing zookeeper connection: " << hosts_;
  CHECK(zoo_ == nullptr) << "Doubly initializing zookeeper";
  CHECK(!hosts_.empty()) << "Passed in an invalid host string";

  rawInitHandle(this);

  LOG(INFO) << "Zookeeper initialized. State: " << getState()
            << ", session id: " << getSessionId();
}

void ZKClient::destroy() {
  if(!zoo_) {
    return;
  }
  int ret = zookeeper_close(zoo_);
  if(ret != ZOK) {
    LOG(ERROR) << "Failed to cleanup ZooKeeper, zookeeper_close: "
               << zerror(ret);
  }
  zoo_ = nullptr;
}

ZKClient::~ZKClient() { destroy(); }

void ZKClient::rawInitHandle(ZKClient *cli) {
  std::lock_guard<std::mutex>(cli->rawInitMutex_);
  if(cli->zoo_ && cli->getState() != ZOO_EXPIRED_SESSION_STATE) {
    return;
  }
  if(cli->zoo_) {
    // This is due to server connection failure. give it a second.
    // On local host testing, this is due to the zookeepr process is out of heap
    // and is doing a major GC compaction. So its useless to try and reconnect
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  cli->ready = false;
  // Idea taken from Zookeper/zookeeper.cpp in mesos
  // We retry zookeeper_init until the timeout elapses because we've
  // seen cases where temporary DNS outages cause the slave to abort
  // here. See MESOS-1326 for more information.
  // ZooKeeper masks EAI_AGAIN as EINVAL and a name resolution timeout
  // may be upwards of 30 seconds. As such, a 10 second timeout is not
  // enough. Hard code this to 10 minutes to be sure we're trying again
  // in the face of temporary name resolution failures. See MESOS-1523
  // for more information.
  int maxInitTries = 600;
  while(maxInitTries-- > 0) {
    cli->zoo_ = zookeeper_init(cli->hosts().c_str(), &watchCb, cli->timeout(),
                               cli->getClientId(), (void *)cli, cli->flags());
    // Unfortunately, EINVAL is highly overloaded in zookeeper_init
    // and can correspond to:
    //   (1) Empty / invalid 'host' string format.
    //   (2) Any getaddrinfo error other than EAI_NONAME,
    //       EAI_NODATA, and EAI_MEMORY are mapped to EINVAL.
    // Either way, retrying is not problematic.
    if(cli->zoo_ == nullptr && errno == EINVAL) {
      LOG(ERROR) << "Error initializing zookeeper. Retrying in 1 second";
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    break;
  }

  if(cli->zoo_ == NULL) {
    PLOG(FATAL) << "Failed to create ZooKeeper, zookeeper_init";
  }

  CHECK(cli->zoo_) << "Failed to initialize zookeeper";

  while(!cli->ready) {
    std::this_thread::yield();
  }
}


static void
watchCb(zhandle_t *zh, int type, int state, const char *cpath, void *ctx) {
  DCHECK(ctx) << "invalid context on the callback";
  ZKClient *self = static_cast<ZKClient *>(ctx);

  if(type == ZOO_SESSION_EVENT) {
    if(state == ZOO_CONNECTED_STATE) {
      LOG(INFO) << "Zookeeper connected...";
      self->ready = true;
    } else if(state == ZOO_ASSOCIATING_STATE) {
      LOG(ERROR) << "Zookeeper associating...";
    } else if(state == ZOO_EXPIRED_SESSION_STATE) {
      LOG(ERROR) << "Zookeeper session expired. ZOO_EXPIRED_SESSION_STATE. "
                    "Attempting to retry session stablishment";
    }
  }
  self->watch_(type, state, std::string(cpath == nullptr ? "" : cpath), self);
}

static void dataCompletionCb(int rc,
                             const char *value,
                             int value_len,
                             const struct Stat *stat,
                             const void *data) {

  auto promise = promiseFromData(data);

  struct ZKResult result(rc, stat ? boost::optional<Stat>(*stat) : boost::none);

  if(value) {
    result.buff = folly::IOBuf::copyBuffer((void *)value, value_len);
  }

  promise->setValue(std::move(result));
}

static void stringsAndStatCompletionCb(int rc,
                                       const struct String_vector *strs,
                                       const struct Stat *stat,
                                       const void *data) {
  auto promise = promiseFromData(data);
  struct ZKResult result(rc, stat ? boost::optional<Stat>(*stat) : boost::none);

  for(auto i = 0; strs && i < strs->count; ++i) {
    result.strings.push_back(strs->data[i]);
  }

  promise->setValue(std::move(result));
}

std::string ZKClient::printZookeeperEventType(int type) {
  if(type == ZOO_CREATED_EVENT) {
    return "ZOO_CREATED_EVENT";
  }

  if(type == ZOO_DELETED_EVENT) {
    return "ZOO_DELETED_EVENT";
  }

  if(type == ZOO_CHANGED_EVENT) {
    return "ZOO_CHANGED_EVENT";
  }

  if(type == ZOO_CHILD_EVENT) {
    return "ZOO_CHILD_EVENT";
  }

  if(type == ZOO_SESSION_EVENT) {
    return "ZOO_SESSION_EVENT";
  }

  if(type == ZOO_NOTWATCHING_EVENT) {
    return "ZOO_NOTWATCHING_EVENT";
  }

  return "UNKNOWN_EVENT: " + std::to_string(type);
}

std::string ZKClient::printZookeeperState(int state) {
  if(state == ZOO_EXPIRED_SESSION_STATE) {
    return "ZOO_EXPIRED_SESSION_STATE";
  }

  if(state == ZOO_AUTH_FAILED_STATE) {
    return "ZOO_AUTH_FAILED_STATE";
  }

  if(state == ZOO_CONNECTING_STATE) {
    return "ZOO_CONNECTING_STATE";
  }

  if(state == ZOO_ASSOCIATING_STATE) {
    return "ZOO_ASSOCIATING_STATE";
  }

  if(state == ZOO_CONNECTED_STATE) {
    return "ZOO_CONNECTED_STATE";
  }

  return "ZOO_UNKNOWN_STATE: " + std::to_string(state);
}

static void
statCompletionCb(int rc, const struct Stat *stat, const void *data) {

  auto promise = promiseFromData(data);
  struct ZKResult result(rc, stat ? boost::optional<Stat>(*stat) : boost::none);
  promise->setValue(std::move(result));
}

Future<ZKResult> ZKClient::get(std::string path, bool watch) {
  Promise<ZKResult> *promise = new Promise<ZKResult>;

  if(!ready) {
    promise->setException(std::runtime_error("Not connected"));
  } else {
    zoo_aget(zoo_, path.c_str(), watch ? 1 : 0, &dataCompletionCb,
             static_cast<void *>(promise));
  }

  return promise->getFuture();
}

const clientid_t *ZKClient::getClientId() {
  if(!zoo_ || getState() == ZSESSIONEXPIRED) {
    return nullptr;
  }
  return zoo_client_id(zoo_);
}


ZKResult ZKClient::getSync(std::string path, bool watch) {
  struct Stat stat;
  int bufLen = 1 << 20; // 1MB is max for zookeeper
  std::unique_ptr<char[]> buf(new char[bufLen]());
  int rc =
    zoo_get(zoo_, path.c_str(), watch ? 1 : 0, buf.get(), &bufLen, &stat);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_get(zoo_, path.c_str(), watch ? 1 : 0, buf.get(), &bufLen, &stat);
  }

  if(rc != ZOK) {
    return ZKResult(rc);
  }

  struct ZKResult result(rc, stat, folly::IOBuf::copyBuffer(buf.get(), bufLen));

  return result;
}

Future<ZKResult> ZKClient::set(std::string path,
                               std::unique_ptr<folly::IOBuf> &&val,
                               int version) {
  Promise<ZKResult> *promise = new Promise<ZKResult>;

  if(!ready) {
    promise->setException(std::runtime_error("Not connected"));
  } else {
    zoo_aset(zoo_, path.c_str(), (char *)val->data(), val->length(), version,
             &statCompletionCb, static_cast<void *>(promise));
  }

  return promise->getFuture();
}

ZKResult ZKClient::setSync(std::string path,
                           std::unique_ptr<folly::IOBuf> &&val,
                           int version) {

  struct Stat stat;
  int rc = zoo_set2(zoo_, path.c_str(), (const char *)val->data(),
                    val->length(), version, &stat);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_set2(zoo_, path.c_str(), (const char *)val->data(), val->length(),
                  version, &stat);
  }
  struct ZKResult result(rc, stat);
  return result;
}

Future<ZKResult> ZKClient::children(std::string path, bool watch) {
  Promise<ZKResult> *promise = new Promise<ZKResult>;

  if(!ready) {
    promise->setException(std::runtime_error("Not connected"));
  } else {
    // zoo_aget_children2(zhandle_t *zh, const char *path, int watch,
    //    strings_stat_completion_t completion, const void *data);
    zoo_aget_children2(zoo_, path.c_str(), watch ? 1 : 0,
                       stringsAndStatCompletionCb,
                       static_cast<void *>(promise));
  }

  return promise->getFuture();
}
ZKResult ZKClient::childrenSync(std::string path, bool watch) {

  struct String_vector strs {
    0, nullptr
  }; //  = nullptr;
  struct Stat stat;
  int rc = zoo_get_children(zoo_, path.c_str(), watch ? 1 : 0, &strs);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_get_children(zoo_, path.c_str(), watch ? 1 : 0, &strs);
  }
  struct ZKResult result(rc, stat);
  for(auto i = 0; strs.data && i < strs.count; ++i) {
    char *ptr = strs.data[i];
    if(ptr) {
      result.strings.push_back(std::string(ptr));
    }
  }
  return result;
}

Future<ZKResult> ZKClient::exists(std::string path, bool watch) {
  Promise<ZKResult> *p = new Promise<ZKResult>;

  if(!ready) {
    p->setException(std::runtime_error("Not connected"));
  } else {
    zoo_aexists(zoo_, path.c_str(), watch ? 1 : 0, &statCompletionCb,
                static_cast<void *>(p));
  }

  return p->getFuture();
}

ZKResult ZKClient::existsSync(std::string path, bool watch) {

  struct Stat stat;
  int rc = zoo_exists(zoo_, path.c_str(), watch ? 1 : 0, &stat);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_exists(zoo_, path.c_str(), watch ? 1 : 0, &stat);
  }
  struct ZKResult result(rc, stat);
  return result;
}

static void stringCompletionCb(int rc, const char *value, const void *data) {
  auto promise = promiseFromData(data);
  struct ZKResult result(rc);

  if(value) {
    result.buff = folly::IOBuf::copyBuffer(
      (void *)value, std::char_traits<char>::length(value));
  }

  promise->setValue(std::move(result));
}

Future<ZKResult> ZKClient::create(std::string path,
                                  std::unique_ptr<folly::IOBuf> &&val,
                                  ACL_vector *acl,
                                  int flags) {
  VLOG(1) << "Create path: " << path;
  Promise<ZKResult> *p = new Promise<ZKResult>;

  if(!ready) {
    p->setException(std::runtime_error("Not connected"));
  } else {
    zoo_acreate(zoo_, path.c_str(), (char *)val->data(), val->length(), acl,
                flags, &stringCompletionCb, static_cast<void *>(p));
  }

  return p->getFuture();
}

ZKResult ZKClient::createSync(std::string path,
                              std::unique_ptr<folly::IOBuf> &&val,
                              ACL_vector *acl,
                              int flags) {

  std::unique_ptr<char[]> pathBuf(new char[1024]());
  int rc = zoo_create(zoo_, path.c_str(), (const char *)val->data(),
                      val->length(), acl, flags, pathBuf.get(), 1024);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_create(zoo_, path.c_str(), (const char *)val->data(),
                    val->length(), acl, flags, pathBuf.get(), 1024);
  }

  struct ZKResult result(
    rc, boost::none,
    folly::IOBuf::copyBuffer(pathBuf.get(),
                             std::char_traits<char>::length(pathBuf.get())));

  return result;
}

static void voidCompletionCb(int rc, const void *data) {

  auto promise = promiseFromData(data);

  struct ZKResult result(rc);
  promise->setValue(std::move(result));
}

Future<ZKResult> ZKClient::del(std::string path, int version) {
  Promise<ZKResult> *p = new Promise<ZKResult>;

  if(!ready) {
    p->setException(std::runtime_error("Not connected"));
  } else {
    zoo_adelete(zoo_, path.c_str(), version, &voidCompletionCb,
                static_cast<void *>(p));
  }

  return p->getFuture();
}

ZKResult ZKClient::delSync(std::string path, int version) {
  int rc = zoo_delete(zoo_, path.c_str(), version);
  int maxTries = kMaxTriesPerSyncOperation;
  while(maxTries-- > 0 && (rc == ZINVALIDSTATE || retryable(rc))) {
    CHECK(getState() != ZOO_AUTH_FAILED_STATE);
    ZKClient::rawInitHandle(this);
    rc = zoo_delete(zoo_, path.c_str(), version);
  }

  struct ZKResult result(rc);
  return result;
}
}
