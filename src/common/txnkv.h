#ifndef TXNKV_H
#define TXNKV_H

#include "libtxnkv.h"
#include "include/Context.h"
#include <dlfcn.h>

class CephContext;

typedef void (*funcPtrFreeKVStruct)(KV_return** p0, int limit);
typedef void (*funcPtrFreeCBytes)(char* p0);

struct KV_s {
  string k;
  string v;
};

class TikvClientOperate {
public:
  CephContext *cct;
  funcPtrFreeKVStruct pfFreeKVStruct;
  funcPtrFreeCBytes pfFreeCBytes;
  void *dp;

public:
  TikvClientOperate(CephContext *_cct, const string& library_path);
  ~TikvClientOperate();
  void initTikvClientOperate(const string& pd_addr);
  int Puts(const map<string, string>& args);
  int PutsMap(const map<string, string>& args);
  int PutsVec(const vector<map<string, string>> v);
  struct KV_s Get(string& key);
  int DelKey(const string& key);
  int DelKeys(const vector<string>& args);
  map<string, string> Scan(const string& keyPrefix, int limit); 
};

#endif
