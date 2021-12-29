#ifndef CEPH_OPERATEKV_H
#define CEPH_OPERATEKV_H

#include "include/Context.h"
#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/txnkv.h"
#include "common/obj_dir_cache.h"

class CephContext;

#define dout_subsys ceph_subsys_operateKV
#undef dout_prefix
#define dout_prefix *_dout << "operateKV "

enum OperateKVOp {
  KV_ADD = 0,
  KV_DEL = 1,
};

struct queue_op_map {
  OperateKVOp op;
  string name;   //only use for delete obj
  map<std::string, bufferlist> queue_map; //only use for write obj
};

class OperateKV {
private:
  CephContext *cct;
  ceph::mutex operateKV_lock;
  ceph::condition_variable operateKV_cond;
  ceph::condition_variable operateKV_empty_cond;
  vector<queue_op_map> operateKV_queue;
  TikvClientOperate *tikvClientOperate;
  ObjDirCache *obj_dir_cache;

  string thread_name;

  bool operateKV_stop;
  bool operateKV_running;
  bool operateKV_empty_wait;

  void *operateKV_thread_entry();

  struct OperateKVThread : public Thread {
    OperateKV *op_kv;
    explicit OperateKVThread(OperateKV *o) : op_kv(o) {}
    void* entry() override { return op_kv->operateKV_thread_entry(); }
  } operateKV_thread;

  string library_path;
public:
  void queue(string opname, const string& name, map<std::string, bufferlist>& m) {
    std::unique_lock ul(operateKV_lock);
	if (operateKV_queue.empty()) {
	  operateKV_cond.notify_all();
	}
	struct queue_op_map qom;
	if (opname == "KV_ADD") {
	  qom.op = KV_ADD;
	  qom.queue_map = m;
	} else if (opname == "KV_DEL") {
	  qom.op = KV_DEL;
	  qom.name = name;
	}
	operateKV_queue.push_back(qom);
  }

  void start();

  void stop();

  void wait_for_empty();

  explicit OperateKV(const string& _library_path, CephContext *_cct) :
    cct(_cct), operateKV_lock("OperateKV::operateKV_lock"), thread_name("operateKV"),
	operateKV_stop(false), operateKV_running(false), operateKV_empty_wait(false), operateKV_thread(this), library_path(_library_path) {
	  tikvClientOperate = new TikvClientOperate(_cct, library_path);
	  string pd_addr = "10.1.172.118:2379";
	  tikvClientOperate->initTikvClientOperate(pd_addr);

	  obj_dir_cache = new ObjDirCache;
	  obj_dir_cache->set_ctx(cct);
	  obj_dir_cache->set_enabled(true);
	}

  ~OperateKV() {
    if (tikvClientOperate) {
	  delete tikvClientOperate;
	  tikvClientOperate = NULL;
    }
  }

  void handle_str(std::string& input, vector<string>& result, bool has_flag);
  void recursive(std::string& input, int nums, int count, vector<string>& result, bool has_flag);
  vector<string> extract(std::string& input);
  bool has_inserted(std::string& head_str);
  std::string replaceAllword(const std::string& resources, const string& key, const std::string& ReplaceKey);
  map<string, bufferlist> getKV(const std::string& obj_name);
  void find_myself(const string& input, string& self);
  map<string, string> scanKV(const string& cur_prefix, int limit);
};

#undef dout_subsys
#endif
