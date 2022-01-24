#ifndef CEPH_OPERATEKV_H
#define CEPH_OPERATEKV_H

#include "include/Context.h"
#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"
#include "common/txnkv.h"
#include "common/obj_dir_cache.h"
#include "include/utime.h"
#include "common/Timer.h"

class CephContext;

#define dout_subsys ceph_subsys_operateKV
#undef dout_prefix
#define dout_prefix *_dout << "operateKV "

enum OperateKVOp {
  KV_ADD = 0,
  KV_DEL = 1,
  KV_DEL_BUCKET = 2,
};

struct queue_op_map {
  OperateKVOp op;
  string name;   //only use for delete obj
  map<std::string, bufferlist> queue_map; //only use for write obj
  vector<map<std::string, bufferlist>> batch_queue_map; //only use for batch write obj
  bool multi_delete; //only use for multi_object delete

  queue_op_map() : multi_delete(false) {}
};

class KVQueueContext : public Context {
public:
  KVQueueContext(boost::function<void(int)> &&callback) 
    : m_callback(std::move(callback)) {
  }

  void finish(int r) override {
	m_callback(r);
  }

private:
  boost::function<void(int)> m_callback;
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
  utime_t add_queue_time;
  long add_queue_num;
  vector<map<string, bufferlist>> batch_add;
  vector<map<string, bufferlist>> batch_add_temp;

  SafeTimer timer;
  Mutex vec_lock;
  Context* kv_queue_event = nullptr;

public:
  void set_add_queue_time_stamp(utime_t t) {
    add_queue_time = t;
  }
  utime_t get_add_queue_time_stamp() const {
    return add_queue_time;
  }

  void queue(string opname, const string& name, map<std::string, bufferlist>& m, bool multi_delete = false) {
    std::unique_lock ul(operateKV_lock);
	if (operateKV_queue.empty()) {
	  operateKV_cond.notify_all();
	}
	struct queue_op_map qom;
	if (opname == "KV_ADD") {
	  vec_lock.Lock();
	  batch_add_temp.emplace_back(m);
	  float timeval = (float)(ceph_clock_now() - get_add_queue_time_stamp());
	  if (timeval < cct->_conf->rgw_operateKV_queue_timeval) {
		//reset old timer
		if (kv_queue_event) {
		  timer.cancel_event(kv_queue_event);
		}
		if (batch_add.empty()) {
		  batch_add = batch_add_temp;
		} else {
		  for (auto &i : batch_add_temp) {
			batch_add.emplace_back(i);
		  }
		}
		batch_add_temp.clear();
		if (batch_add.size() >= (unsigned)cct->_conf->rgw_operateKV_queue_max_size) {
		  //do it now
		  qom.op = KV_ADD;
		  qom.batch_queue_map = batch_add;
		  batch_add.clear();
		  operateKV_queue.push_back(qom);
		  vec_lock.Unlock();
		  return;
		}
	  } else {
		if (batch_add.empty()) {
		  batch_add = batch_add_temp;
		  batch_add_temp.clear();
		}
	  }

	  set_add_queue_time_stamp(ceph_clock_now());
	  kv_queue_event = timer.add_event_after(cct->_conf->rgw_operateKV_queue_timeval, new KVQueueContext([this](int) {
		std::unique_lock ul(operateKV_lock);
		if (operateKV_queue.empty()) {
		  operateKV_cond.notify_all();
		}
		if (!batch_add.empty()) {
		  if (!batch_add_temp.empty()) {
			for (auto &i : batch_add_temp) {
			  batch_add.emplace_back(i);
			}
			batch_add_temp.clear();
		  }
		  struct queue_op_map qomb;
		  qomb.op = KV_ADD;
		  qomb.batch_queue_map = batch_add;
		  batch_add.clear();
		  operateKV_queue.push_back(qomb);
		}
	  }));
	  vec_lock.Unlock();
	} else if (opname == "KV_DEL") {
	  qom.op = KV_DEL;
	  qom.name = name;
	  qom.multi_delete = multi_delete;
	  operateKV_queue.push_back(qom);
	} else if (opname == "KV_DEL_BUCKET") {
	  qom.op = KV_DEL_BUCKET;
	  qom.name = name;
	  operateKV_queue.push_back(qom);
	}
  }

  void start();

  void stop();

  void wait_for_empty();

  explicit OperateKV(const string& _library_path, CephContext *_cct) :
    cct(_cct), operateKV_lock(ceph::make_mutex("OperateKV::operateKV_lock")), thread_name("operateKV"),
	operateKV_stop(false), operateKV_running(false), operateKV_empty_wait(false), operateKV_thread(this), library_path(_library_path),
	timer(_cct, vec_lock), vec_lock("OperateKV::vec_lock") {
	  tikvClientOperate = new TikvClientOperate(_cct, library_path);
	  string pd_addr = "10.1.172.118:2379";
	  tikvClientOperate->initTikvClientOperate(pd_addr);

	  obj_dir_cache = new ObjDirCache;
	  obj_dir_cache->set_ctx(cct);
	  obj_dir_cache->set_enabled(true);
	  set_add_queue_time_stamp(ceph_clock_now());
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
  bool parent_dir_check(const string& parent_dir);
  void delete_dir_cache(std::string& head_str);
  map<string, string> scanKV(const string& cur_prefix, int limit);
};

#undef dout_subsys
#endif
