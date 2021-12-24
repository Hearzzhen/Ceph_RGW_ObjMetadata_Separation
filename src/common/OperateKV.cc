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

  if (nums == 1) {
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

bool OperateKV::has_inserted(std::string head_str) {
  //TODO
  return false;
}

void *OperateKV::operateKV_thread_entry() {
  std::unique_lock ul(operateKV_lock);
  ldout(cct, 10) << "operateKV_thread start." << dendl;

  while(!operateKV_stop) {
	while(!operateKV_queue.empty()) {
	  vector<map<std::string, bufferlist>> ls;
	  ls.swap(operateKV_queue);
	  operateKV_running = true;
	  ul.unlock();
	  ldout(cct, 10) << "operateKV_thread doing." << dendl;

	  for (auto p : ls) {
		//extract obj name, and determine the relationship.
		map<std::string, bufferlist>::iterator iter = p.begin();
		string obj_name = iter->first;
		vector<string> extract_res;
		extract_res = extract(obj_name);
#if 1
		ldout(cct, 10) << "print extract_res: " << dendl;
		for (auto &i : extract_res) {
		  ldout(cct, 10) << i << dendl;
		}
#endif
		//TODO: extract result should log into LRU or other data structure, avoid to insert to tikv twice and more.
		for (auto &i : extract_res) {
		  string _str = i;
		  if (has_inserted(_str)) 
			continue;
		  if (_str[_str.length() - 1] == '-') {
			map<string, string> args;
			args.emplace(pair<string, string>(_str, "head"));
		//	tikvClientOperate.Puts(args);
			ldout(cct, 10) << "put head." << dendl;
			for (auto &i : args) {
			  ldout(cct, 10) << "key = " << i.first << " val = " << i.second << dendl;
			}
		  } else if (_str[_str.length() - 1] == '~') {
			map<string, string> args;
			args.emplace(pair<string, string>(_str, "tail"));
			ldout(cct, 10) << "put tail." << dendl;
		//	tikvClientOperate.Puts(args);
			for (auto &i : args) {
			  ldout(cct, 10) << "key = " << i.first << " val = " << i.second << dendl;
			}
		  } else {
			map<std::string, bufferlist> args;
			for (auto &i : p) {
			  //TODO
			  string m_name = _str;
			  args.emplace(pair<string, bufferlist>(m_name, i.second));
			}
			ldout(cct, 10) << "put objmeta!" << dendl;
		//	tikvClientOperate.Puts(args); //need increase the tikv Puts interface
			for (auto &i : args) {
			  ldout(cct, 10) << "key = " << i.first << dendl;//" val = " << i.second << dendl;
			  map<string, bufferlist> m1;
			  decode(m1, i.second);
			  for (auto &j : m1) {
				ldout(cct, 10) << "m_k = " << j.first << dendl;
			    if (j.first == "user.rgw.x-amz-meta-s3cmd-attrs") {
				  string t;
				  //decode(t, j.second);
				  t = j.second.to_str();
				  ldout(cct, 10) << "when key = user.rgw.x-amz-meta-s3cmd-attrs, decode val = " << t << dendl;
			    }
			    if (j.first == "omapvals") {
				  obj_omap oo(cct);
				  decode(oo, j.second);
				  ldout(cct, 10) << "when key = oo.epoch, val = " << oo.epoch << dendl;
			    }
			  }
			}
		  }
		}
	  }
	  ldout(cct, 10) << "operateKV_thread done with " << ls << dendl;
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

