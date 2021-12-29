#include "txnkv.h"

#define dout_subsys ceph_subsys_operateKV
#undef dout_prefix
#define dout_prefix *_dout << "operateKV(" << this << ") "

GoString buildGoString(const char* p, size_t n){
	return {p, static_cast<ptrdiff_t>(n)};
}

TikvClientOperate::TikvClientOperate(CephContext* _cct, const string& library_path) : cct(_cct) {
  ldout(cct, 0) << "TikvClientOperate construct, library_path = " << library_path.c_str() << dendl;
  dp = dlopen(library_path.c_str(), RTLD_LAZY);
  if (NULL == dp)
  {
    ldout(cct, 0) << "TikvClientOperate read library error! " << dlerror() << dendl;
  }

  pfFreeKVStruct = (funcPtrFreeKVStruct)dlsym(dp, "FreeKVStruct");
  pfFreeCBytes = (funcPtrFreeCBytes)dlsym(dp, "FreeCBytes");
  ldout(cct, 0) << "TikvClientOperate construct complete!" << dendl;
}

TikvClientOperate::~TikvClientOperate() {
  if (dp) {
    dlclose(dp);
  }
}

void TikvClientOperate::initTikvClientOperate(const string& pd_addr) {
  ldout(cct, 0) << "TikvClientOperate::initTikvClientOperate in." << dendl;
  GoString s_pd_addr = buildGoString(pd_addr.c_str(), pd_addr.size());
  initStore(s_pd_addr);
  ldout(cct, 0) << "initStore finish." << dendl;
}

int TikvClientOperate::Puts(const map<string, string>& args) {
  string key, val;
  GoInt r = 0;
  for (auto &i : args) {
    key = i.first;
    val = i.second;
    r = putsKV(buildGoString(key.c_str(), key.size()), buildGoString(val.c_str(), val.size()));
  }
  return r;
}

int TikvClientOperate::PutsMap(const map<string, string>& args) {
  int size = args.size();
  KV_return** kv;
  kv = (KV_return**)malloc(sizeof(KV_return*) * size); 
  map<string, string>::const_iterator iter = args.begin();
  for (int i = 0; i < size && iter != args.end(); ++i) {
    string temp_k = iter->first;
    string temp_v = iter->second;
    kv[i] = (KV_return*)malloc(sizeof(KV_return));
    kv[i]->k = (unsigned char*)malloc(sizeof(unsigned char) * temp_k.length());
    kv[i]->v = (unsigned char*)malloc(sizeof(unsigned char) * temp_v.length());
    memcpy(kv[i]->k, temp_k.c_str(), temp_k.length());
    memcpy(kv[i]->v, temp_v.c_str(), temp_v.length());
    iter++;
  }
  GoInt r = putsKVMap(kv, GoInt(size));
  if (r == 0) {
    ldout(cct, 10) << "PutsMap Success!" << dendl;
  } else {
    ldout(cct, 0) << "PutsMap Failed!" << dendl;
  }

  if(kv != 0){
    for (int i = 0; i < size; ++i) {
      if(kv[i]->k != 0) {
        free(kv[i]->k);
        kv[i]->k = NULL;
        ldout(cct, 10) << "PutsMap End! Free " << i << " key" << dendl;
      }
      if(kv[i]->v != 0) {
        free(kv[i]->v);
        kv[i]->v = NULL;
        ldout(cct, 10) << "PutsMap End! Free " << i << " val" << dendl;
      }
      free(kv[i]);
      kv[i] = NULL;
      ldout(cct, 10) << "PutsMap End! Free kv!" << dendl;
    }
  }
  return r;
}

struct KV_s TikvClientOperate::Get(string& key) {
  GoString s_key = buildGoString(key.c_str(), key.size());
  struct getKV_return res;
  res = getKV(s_key);
  struct KV_s kv_s;
  kv_s.k = key;
  //kv_s.v = res.r0;
  kv_s.v.append(reinterpret_cast<const char*>(res.r0), res.r1);
  ldout(cct, 10) << "TikvClientOperate::Get Key = " << key << " Val = " << res.r0 << dendl;

  funcPtrFreeCBytes(res.r0); 
  return kv_s;
}

map<string, string> TikvClientOperate::Scan(const string& keyPrefix, int limit) {
  map<string, string> m;
  struct scanKV_return res;
  res = scanKV(buildGoString(keyPrefix.c_str(), keyPrefix.size()), limit);
  if (res.r2 == 0) {
	if (res.r1 <= limit) {
	  limit = res.r1;
	  ldout(cct, 10) << "limit: " << limit << dendl;
	}
    for (int i = 0; i < limit; ++i) {
	  string sk, sv;
	  sk.append(reinterpret_cast<const char*>(res.r0[i]->k), res.r0[i]->klen);
	  sv.append(reinterpret_cast<const char*>(res.r0[i]->v), res.r0[i]->vlen);
      m.emplace(sk, sv);
      ldout(cct, 10) << sk << " ==> length: " << sv.length() << " was insert into map!" << dendl;
    }
  }

  pfFreeKVStruct(res.r0, limit);
  return m; 
}

int TikvClientOperate::DelKey(const string& key) {
  int r = 0;
  r = delKey(buildGoString(key.c_str(), key.size()));
  return r;
}
int TikvClientOperate::DelKeys(const vector<string>& args) {
  int r = 0;
  int size = args.size();
  char** keys_str = (char**)malloc(sizeof(char*) * size);
  vector<string>::const_iterator iter = args.begin();
  for (int i = 0; i < size && iter != args.end(); ++i, ++iter) {
    keys_str[i] = (char*)malloc(sizeof(char) * (*iter).length());
	memset(keys_str[i], 0, (*iter).length());
    memcpy(keys_str[i], (*iter).c_str(), (*iter).length());
  }
  r = delKeys(keys_str, size);
  if (keys_str) {
    for (int i = 0; i < size && iter != args.begin(); ++i) {
      if (keys_str[i]) {
        free(keys_str[i]);
        keys_str[i] = NULL;
      }
    }
    free(keys_str);
    keys_str = NULL;
  }

  return r;
}
