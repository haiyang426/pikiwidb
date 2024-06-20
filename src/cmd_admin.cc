/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_admin.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "db.h"

#include "braft/raft.h"
#include "pstd_string.h"
#include "rocksdb/version.h"

#include "pikiwidb.h"
#include "praft/praft.h"
#include "pstd/env.h"

#include "store.h"

namespace pikiwidb {

CmdConfig::CmdConfig(const std::string& name, int arity) : BaseCmdGroup(name, kCmdFlagsAdmin, kAclCategoryAdmin) {}

bool CmdConfig::HasSubCommand() const { return true; }

CmdConfigGet::CmdConfigGet(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin) {}

bool CmdConfigGet::DoInitial(PClient* client) { return true; }

void CmdConfigGet::DoCmd(PClient* client) {
  std::vector<std::string> results;
  for (int i = 0; i < client->argv_.size() - 2; i++) {
    g_config.Get(client->argv_[i + 2], &results);
  }
  client->AppendStringVector(results);
}

CmdConfigSet::CmdConfigSet(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin, kAclCategoryAdmin) {}

bool CmdConfigSet::DoInitial(PClient* client) { return true; }

void CmdConfigSet::DoCmd(PClient* client) {
  auto s = g_config.Set(client->argv_[2], client->argv_[3]);
  if (!s.ok()) {
    client->SetRes(CmdRes::kInvalidParameter);
  } else {
    client->SetRes(CmdRes::kOK);
  }
}

FlushdbCmd::FlushdbCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsExclusive | kCmdFlagsAdmin | kCmdFlagsWrite,
              kAclCategoryWrite | kAclCategoryAdmin) {}

bool FlushdbCmd::DoInitial(PClient* client) { return true; }

void FlushdbCmd::DoCmd(PClient* client) {
  int currentDBIndex = client->GetCurrentDB();
  PSTORE.GetBackend(currentDBIndex).get()->Lock();

  std::string db_path = g_config.db_path.ToString() + std::to_string(currentDBIndex);
  std::string path_temp = db_path;
  path_temp.append("_deleting/");
  pstd::RenameFile(db_path, path_temp);

  auto s = PSTORE.GetBackend(currentDBIndex)->Open();
  assert(s.ok());
  auto f = std::async(std::launch::async, [&path_temp]() { pstd::DeleteDir(path_temp); });
  PSTORE.GetBackend(currentDBIndex).get()->UnLock();
  client->SetRes(CmdRes::kOK);
}

FlushallCmd::FlushallCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsExclusive | kCmdFlagsAdmin | kCmdFlagsWrite,
              kAclCategoryWrite | kAclCategoryAdmin) {}

bool FlushallCmd::DoInitial(PClient* client) { return true; }

void FlushallCmd::DoCmd(PClient* client) {
  for (size_t i = 0; i < g_config.databases; ++i) {
    PSTORE.GetBackend(i).get()->Lock();
    std::string db_path = g_config.db_path.ToString() + std::to_string(i);
    std::string path_temp = db_path;
    path_temp.append("_deleting/");
    pstd::RenameFile(db_path, path_temp);

    auto s = PSTORE.GetBackend(i)->Open();
    assert(s.ok());
    auto f = std::async(std::launch::async, [&path_temp]() { pstd::DeleteDir(path_temp); });
    PSTORE.GetBackend(i).get()->UnLock();
  }
  client->SetRes(CmdRes::kOK);
}

SelectCmd::SelectCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsReadonly, kAclCategoryAdmin) {}

bool SelectCmd::DoInitial(PClient* client) { return true; }

void SelectCmd::DoCmd(PClient* client) {
  int index = atoi(client->argv_[1].c_str());
  if (index < 0 || index >= g_config.databases) {
    client->SetRes(CmdRes::kInvalidIndex, kCmdNameSelect + " DB index is out of range");
    return;
  }
  client->SetCurrentDB(index);
  client->SetRes(CmdRes::kOK);
}

ShutdownCmd::ShutdownCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin | kAclCategoryWrite) {}

bool ShutdownCmd::DoInitial(PClient* client) {
  // For now, only shutdown need check local
  if (client->PeerIP().find("127.0.0.1") == std::string::npos &&
      client->PeerIP().find(g_config.ip.ToString()) == std::string::npos) {
    client->SetRes(CmdRes::kErrOther, kCmdNameShutdown + " should be localhost");
    return false;
  }
  return true;
}

void ShutdownCmd::DoCmd(PClient* client) {
  PSTORE.GetBackend(client->GetCurrentDB())->UnLockShared();
  g_pikiwidb->Stop();
  PSTORE.GetBackend(client->GetCurrentDB())->LockShared();
  client->SetRes(CmdRes::kNone);
}

PingCmd::PingCmd(const std::string& name, int16_t arity) : BaseCmd(name, arity, kCmdFlagsFast, kAclCategoryFast) {}

bool PingCmd::DoInitial(PClient* client) { return true; }

void PingCmd::DoCmd(PClient* client) { client->SetRes(CmdRes::kPong, "PONG"); }

InfoCmd::InfoCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsReadonly, kAclCategoryAdmin) {}

bool InfoCmd::DoInitial(PClient* client) { return true; }

// @todo The info raft command is only supported for the time being
void InfoCmd::DoCmd(PClient* client) {
  if (client->argv_.size() <= 1) {
    return client->SetRes(CmdRes::kWrongNum, client->CmdName());
  }

  auto cmd = client->argv_[1];
  if (!strcasecmp(cmd.c_str(), "RAFT")) {
    InfoRaft(client);
  } else if (!strcasecmp(cmd.c_str(), "data")) {
    InfoData(client);
  } else {
    client->SetRes(CmdRes::kErrOther, "the cmd is not supported");
  }
}

/*
* INFO raft
* Querying Node Information.
* Reply:
*   raft_node_id:595100767
    raft_state:up
    raft_role:follower
    raft_is_voting:yes
    raft_leader_id:1733428433
    raft_current_term:1
    raft_num_nodes:2
    raft_num_voting_nodes:2
    raft_node1:id=1733428433,state=connected,voting=yes,addr=localhost,port=5001,last_conn_secs=5,conn_errors=0,conn_oks=1
*/
void InfoCmd::InfoRaft(PClient* client) {
  if (client->argv_.size() != 2) {
    return client->SetRes(CmdRes::kWrongNum, client->CmdName());
  }

  if (!PRAFT.IsInitialized()) {
    return client->SetRes(CmdRes::kErrOther, "Don't already cluster member");
  }

  auto node_status = PRAFT.GetNodeStatus();
  if (node_status.state == braft::State::STATE_END) {
    return client->SetRes(CmdRes::kErrOther, "Node is not initialized");
  }

  std::string message;
  message += "raft_group_id:" + PRAFT.GetGroupID() + "\r\n";
  message += "raft_node_id:" + PRAFT.GetNodeID() + "\r\n";
  message += "raft_peer_id:" + PRAFT.GetPeerID() + "\r\n";
  if (braft::is_active_state(node_status.state)) {
    message += "raft_state:up\r\n";
  } else {
    message += "raft_state:down\r\n";
  }
  message += "raft_role:" + std::string(braft::state2str(node_status.state)) + "\r\n";
  message += "raft_leader_id:" + node_status.leader_id.to_string() + "\r\n";
  message += "raft_current_term:" + std::to_string(node_status.term) + "\r\n";

  if (PRAFT.IsLeader()) {
    std::vector<braft::PeerId> peers;
    auto status = PRAFT.GetListPeers(&peers);
    if (!status.ok()) {
      return client->SetRes(CmdRes::kErrOther, status.error_str());
    }

    for (int i = 0; i < peers.size(); i++) {
      message += "raft_node" + std::to_string(i) + ":addr=" + butil::ip2str(peers[i].addr.ip).c_str() +
                 ",port=" + std::to_string(peers[i].addr.port) + "\r\n";
    }
  }

  client->AppendString(message);
}

void InfoCmd::InfoData(PClient* client) {
  if (client->argv_.size() != 2) {
    return client->SetRes(CmdRes::kWrongNum, client->CmdName());
  }

  std::string message;
  message += DATABASES_NUM + std::string(":") + std::to_string(pikiwidb::g_config.databases) + "\r\n";
  message += ROCKSDB_NUM + std::string(":") + std::to_string(pikiwidb::g_config.db_instance_num) + "\r\n";
  message += ROCKSDB_VERSION + std::string(":") + ROCKSDB_NAMESPACE::GetRocksVersionAsString() + "\r\n";

  client->AppendString(message);
}

CmdDebug::CmdDebug(const std::string& name, int arity) : BaseCmdGroup(name, kCmdFlagsAdmin, kAclCategoryAdmin) {}

bool CmdDebug::HasSubCommand() const { return true; }

CmdDebugHelp::CmdDebugHelp(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin) {}

bool CmdDebugHelp::DoInitial(PClient* client) { return true; }

void CmdDebugHelp::DoCmd(PClient* client) { client->AppendStringVector(debugHelps); }

CmdDebugOOM::CmdDebugOOM(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin) {}

bool CmdDebugOOM::DoInitial(PClient* client) { return true; }

void CmdDebugOOM::DoCmd(PClient* client) {
  auto ptr = ::operator new(std::numeric_limits<unsigned long>::max());
  ::operator delete(ptr);
  client->SetRes(CmdRes::kErrOther);
}

CmdDebugSegfault::CmdDebugSegfault(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin) {}

bool CmdDebugSegfault::DoInitial(PClient* client) { return true; }

void CmdDebugSegfault::DoCmd(PClient* client) {
  auto ptr = reinterpret_cast<int*>(0);
  *ptr = 0;
}

SortCmd::SortCmd(const std::string& name, int16_t arity)
    : BaseCmd(name, arity, kCmdFlagsAdmin | kCmdFlagsWrite, kAclCategoryAdmin) {}

bool SortCmd::DoInitial(PClient* client) {
  client->SetKey(client->argv_[1]);
  return true;
}

void SortCmd::DoCmd(PClient* client) {
  // const auto& argv = client->argv_;
  int desc = 0;
  int alpha = 0;

  size_t offset = 0;
  size_t count = -1;

  int dontsort = 0;
  int vectorlen;

  int getop = 0;

  std::string store_key;
  std::string sortby;

  std::vector<std::string> get_patterns;
  size_t argc = client->argv_.size();
  DEBUG("argc: {}", argc);
  for (int i = 2; i < argc; ++i) {
    // const auto& arg = pstd::StringToLower(argv[i]);
    int leftargs = argc - i - 1;
    if (strcasecmp(client->argv_[i].data(), "asc") == 0) {
      desc = 0;
    } else if (strcasecmp(client->argv_[i].data(), "desc") == 0) {
      desc = 1;
    } else if (strcasecmp(client->argv_[i].data(), "alpha") == 0) {
      alpha = 1;
    } else if (strcasecmp(client->argv_[i].data(), "limit") == 0 && leftargs >= 2) {
      if (pstd::String2int(client->argv_[i + 1], &offset) == 0 || pstd::String2int(client->argv_[i + 2], &count) == 0) {
        client->SetRes(CmdRes::kSyntaxErr);
        return;
      }
      i += 2;
    } else if (strcasecmp(client->argv_[i].data(), "store") == 0 && leftargs >= 1) {
      store_key = client->argv_[i + 1];
      i++;
    } else if (strcasecmp(client->argv_[i].data(), "by") == 0 && leftargs >= 1) {
      sortby = client->argv_[i + 1];
      if (sortby.find('*') == std::string::npos) {
        dontsort = 1;
      }
      i++;
    } else if (strcasecmp(client->argv_[i].data(), "get") == 0 && leftargs >= 1) {
      get_patterns.push_back(client->argv_[i + 1]);
      getop++;
      i++;
    } else {
      client->SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }

  DEBUG("finish parser ");

  std::vector<std::string> types(1);
  rocksdb::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->GetType(client->Key(), true, types);

  if (!s.ok()) {
    client->SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  std::vector<std::string> ret;
  if (types[0] == "list") {
    storage::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->LRange(client->Key(), 0, -1, &ret);
  } else if (types[0] == "set") {
    storage::Status s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->SMembers(client->Key(), &ret);
  } else if (types[0] == "zset") {
    std::vector<storage::ScoreMember> score_members;
    storage::Status s =
        PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->ZRange(client->Key(), 0, -1, &score_members);
    char buf[32];
    int64_t score_len = 0;

    for (auto& c : score_members) {
      ret.emplace_back(c.member);
    }
  } else {
    client->SetRes(CmdRes::kErrOther, "WRONGTYPE Operation against a key holding the wrong kind of value");
    return;
  }
  DEBUG("finish collect ret ");

  std::vector<RedisSortObject> sort_ret(ret.size());
  for (size_t i = 0; i < ret.size(); ++i) {
    sort_ret[i].obj = ret[i];
  }

  if (!dontsort) {
    for (size_t i = 0; i < ret.size(); ++i) {
      std::string byval;
      if (!sortby.empty()) {
        auto lookup = lookupKeyByPattern(client, sortby, ret[i]);
        if (!lookup.has_value()) {
          byval = ret[i];
        } else {
          byval = std::move(lookup.value());
        }
      } else {
        byval = ret[i];
      }

      if (alpha) {
        sort_ret[i].u = byval;
      } else {
        // auto double_byval = pstd::String2d()
        double double_byval;
        if (pstd::String2d(byval, &double_byval)) {
          sort_ret[i].u = double_byval;
        } else {
          client->SetRes(CmdRes::kErrOther, "One or more scores can't be converted into double");
          return;
        }
      }
    }

    std::sort(sort_ret.begin(), sort_ret.end(), [&alpha, &desc](const RedisSortObject& a, const RedisSortObject& b) {
      if (alpha) {
        std::string score_a = std::get<std::string>(a.u);
        std::string score_b = std::get<std::string>(b.u);
        return !desc ? score_a < score_b : score_a > score_b;
      } else {
        double score_a = std::get<double>(a.u);
        double score_b = std::get<double>(b.u);
        return !desc ? score_a < score_b : score_a > score_b;
      }
    });

    DEBUG("finish sort ret ");
    size_t sort_size = sort_ret.size();

    count = count >= 0 ? count : sort_size;
    offset = (offset >= 0 && offset < sort_size) ? offset : sort_size;
    count = (offset + count < sort_size) ? count : sort_size - offset;

    size_t m_start = offset;
    size_t m_end = offset + count;

    ret.clear();
    if (get_patterns.empty()) {
      get_patterns.emplace_back("#");
    }

    for (; m_start < m_end; m_start++) {
      for (const std::string& pattern : get_patterns) {
        std::optional<std::string> val = lookupKeyByPattern(client, pattern, sort_ret[m_start].obj);
        if (val.has_value()) {
          ret.push_back(val.value());
        } else {
          ret.emplace_back("");
        }
      }
    }
  }

  client->AppendStringVector(ret);

  DEBUG("finish print ");
  // if(dontsort && types[0] == "set"){
  //   dontsort=0;
  //   alpha=1;
  //   sortby.clear();
  // }
}

std::optional<std::string> SortCmd::lookupKeyByPattern(PClient* client, const std::string& pattern,
                                                       const std::string& subst) {
  if (pattern == "#") {
    return subst;
  }

  auto match_pos = pattern.find('*');
  if (match_pos == std::string::npos) {
    return std::nullopt;
  }

  std::string field;
  auto arrow_pos = pattern.find("->", match_pos + 1);
  if (arrow_pos != std::string::npos && arrow_pos + 2 < pattern.size()) {
    field = pattern.substr(arrow_pos + 2);
  }

  std::string key = pattern.substr(0, match_pos + 1);
  key.replace(match_pos, 1, subst);

  std::string value;
  storage::Status s;
  if (!field.empty()) {
    s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->HGet(key, field, &value);
  } else {
    s = PSTORE.GetBackend(client->GetCurrentDB())->GetStorage()->Get(key, &value);
  }

  if (!s.ok()) {
    return std::nullopt;
  }

  return value;
}
}  // namespace pikiwidb
