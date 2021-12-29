#include "OperateKV.h"
#include "rgw/rgw_objmeta.h"

#define dout_subsys ceph_subsys_operateKV
#undef dout_prefix
#define dout_prefix *_dout << "operateKV(" << this << ") "

void OperateKV::start() {
  ldout(cct, 10) << __func__ << dendl;
  operateKV_thread.create(thread_name.c_str());
}

void OperateKV::stop() {
  ldout(cct, 10) << __func__ << dendl;
  operateKV_lock.lock();
  operateKV_stop = true;
  operateKV_cond.notify_all();
  operateKV_lock.unlock();
  operateKV_thread.join();
  ldout(cct, 10) << __func__ << " finish." << dendl;
}

void OperateKV::wait_for_empty() {
  std::unique_lock ul(operateKV_lock);
  while (!operateKV_queue.empty() || operateKV_running) {
    ldout(cct, 10) << "wait_for_empty waiting." << dendl;
	operateKV_empty_wait = true;
	operateKV_empty_cond.wait(ul);
  }

  ldout(cct, 10) << "wait_for_empty empty." << dendl;
  operateKV_empty_wait = false;
}

void OperateKV::handle_str(string& input, vector<string>& result, bool has_flag) {
  size_t position = input.rfind("/");
  string pre_str = input.substr(0, position + 1);
  string post_str = input.substr(position + 1, input.length() - 1);
  result.emplace_back(pre_str + "-");
  if (has_flag) {
	result.emplace_back(pre_str + "." + post_str + "/");
  } else {
	result.emplace_back(pre_str + "." + post_str);
  }
  result.emplace_back(pre_str + "~");
}

void OperateKV::recursive(string& input, int nums, int count, vector<string>& result, bool has_flag) {
  if (nums == count) {
	handle_str(input, result, has_flag);
  }

  if (nums == 2) {
	return;
  }

  size_t position = input.rfind("/");
  input = input.substr(0, position);
  handle_str(input, result, true);
  recursive(input, nums - 1, count, result, true);
}

vector<string> OperateKV::extract(string& input) {
  bool has_flag = false;
  if (input[input.length() - 1] == '/') {
	input = input.substr(0, input.length() - 1);
	has_flag = true;
  }

  int count = 0;
  for (unsigned i = 0; i < input.length(); i++) {
	if (input[i] == '/') {
	  count++;
	}
  }
  vector<string> res;
  recursive(input, count, count, res, has_flag);
  return res;
}

std::string OperateKV::replaceAllword(const std::string& resources, const string& key, const std::string& ReplaceKey)
{
  size_t pos = 0;
  std::string temp = resources;
  while((pos = temp.find(key,pos)) != string::npos)
  {
    temp.erase(pos, key.size()); // delete origin string 
    temp.insert(pos, ReplaceKey); // insert replace string
    pos += ReplaceKey.size(); // update start position
  }
  return temp;
}

bool OperateKV::has_inserted(std::string& head_str) {
  int get_res = obj_dir_cache->get(head_str);
  if (-ENOENT == get_res) {
	ldout(cct, 10) << "not found " << head_str << " in dir cache." << dendl;
	obj_dir_cache->put(head_str);
	return false;
  } else {
	ldout(cct, 10) << "found " << head_str << " in dir cache." << dendl;
	return true;
  }
}

map<string, bufferlist> OperateKV::getKV(const std::string& obj_name) {
  map<string, bufferlist> meta_map;
  string convert_obj_name;
  size_t position = obj_name.rfind("/");
  if (position == obj_name.length() - 1) {
	string tmp_name = obj_name.substr(0, position);
	position = tmp_name.rfind("/");
	string pre_str = tmp_name.substr(0, position + 1);
	string post_str = tmp_name.substr(position + 1, tmp_name.length() - 1);
	convert_obj_name = pre_str + "." + post_str + "/";
  } else {
	string pre_str = obj_name.substr(0, position + 1);
	string post_str = obj_name.substr(position + 1, obj_name.length() - 1);
	convert_obj_name = pre_str + "." + post_str;
  }
  struct KV_s kv_s;
  kv_s = tikvClientOperate->Get(convert_obj_name);
  if (kv_s.v != "") {
    bufferlist out;
    out.append(kv_s.v);
    decode(meta_map, out);
  }
  return meta_map;
}

void OperateKV::find_myself(const string& input, string& self) {
  size_t position;
  if (input[input.length() - 1] == '/') {
	position = input.rfind('/', input.length() - 2);
  } else {
	position = input.rfind('/');
  }
  string pre = input.substr(0, position + 1);
  string post = input.substr(position + 1, input.length() - 1);
  self = pre + "." + post;
}

void *OperateKV::operateKV_thread_entry() {
  std::unique_lock ul(operateKV_lock);
  ldout(cct, 10) << "operateKV_thread start." << dendl;

  while(!operateKV_stop) {
	while(!operateKV_queue.empty()) {
	  vector<queue_op_map> ls;
	  ls.swap(operateKV_queue);
	  operateKV_running = true;
	  ul.unlock();
	  ldout(cct, 10) << "operateKV_thread doing." << dendl;

	  for (auto p : ls) {
		queue_op_map qom = p;
		if (qom.op == KV_ADD) {
		  //extract obj name, and determine the relationship.
		  map<std::string, bufferlist> objmeta_map = qom.queue_map;
		  map<std::string, bufferlist>::iterator iter = objmeta_map.begin();
		  string obj_name = iter->first;
		  string self;
		  find_myself(obj_name, self);
		  vector<string> extract_res;
		  extract_res = extract(obj_name);
		  //TODO: extract result should log into LRU or other data structure, avoid to insert to tikv twice and more.
		  for (auto &i : extract_res) {
		    string _str = i;
		    if (_str[_str.length() - 1] == '-') {
			  if (has_inserted(_str)) {
			    continue;
			  }
			  map<string, string> args;
			  args.emplace(pair<string, string>(_str, "head"));
			  tikvClientOperate->Puts(args);
			  ldout(cct, 10) << "put head." << dendl;
			  for (auto &i : args) {
			    ldout(cct, 10) << "key = " << i.first << " val = " << i.second << dendl;
			  }
		    } else if (_str[_str.length() - 1] == '~') {
			  if (has_inserted(_str)) {
			    continue;
			  }
			  map<string, string> args;
			  args.emplace(pair<string, string>(_str, "tail"));
			  ldout(cct, 10) << "put tail." << dendl;
			  tikvClientOperate->Puts(args);
			  for (auto &i : args) {
			    ldout(cct, 10) << "key = " << i.first << " val = " << i.second << dendl;
			  }
		    } else {
			  map<std::string, bufferlist> args;
			  map<std::string, std::string> args1;
			  if (self == _str) {
			    for (auto &j : objmeta_map) {
			      //TODO
			      string m_name = _str;
			      string b_to_s = string(j.second.to_str());
			      args1.emplace(pair<string, string>(m_name, b_to_s));
			    }
			    ldout(cct, 10) << "put objmeta: " << _str << " to tikv!" << dendl;
			    tikvClientOperate->Puts(args1); //need increase the tikv Puts interface
			  } else {
			    if (has_inserted(_str)) {
				  continue;
			    }
			    args1.emplace(pair<string, string>(_str, "parent_dir"));
			    tikvClientOperate->Puts(args1);
			    ldout(cct, 10) << "put parent dir: " << _str << " to tikv!" << dendl;
			  }
		    }
		  }
	    } else if (qom.op == KV_DEL) {
		  string del_name;
		  find_myself(qom.name, del_name);
		  //TODO: delete dir??? need delete dir cache. 
		  int del_ret = tikvClientOperate->DelKey(del_name);
		  if (del_ret != 0) {
			ldout(cct, 10) << "delete " << del_name << " failed!" << dendl;
		  } else {
			ldout(cct, 10) << "delete " << del_name << " success!" << dendl;
		  }
		}
	  }
	  
	  ldout(cct, 10) << "operateKV_thread done with " << &ls << dendl;
	  ls.clear();
	  ul.lock();
	  operateKV_running = false; 
	}
	ldout(cct, 10) << "operateKV_thread empty." << dendl;
	if (unlikely(operateKV_empty_wait)) {
	  operateKV_empty_cond.notify_all();
	}
	if (operateKV_stop) {
	  break;
	}
	ldout(cct, 10) << "operateKV_thread sleeping." << dendl;
	operateKV_cond.wait(ul);
  }
  operateKV_empty_cond.notify_all();
  ldout(cct, 10) << "operateKV_thread stop." << dendl;
  operateKV_stop = false;
  return 0;
}

