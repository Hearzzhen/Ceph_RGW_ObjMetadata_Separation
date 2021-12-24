#include "rgw_objmeta.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

RGW_ObjMeta::RGW_ObjMeta(CephContext* _cct) : cct(_cct) {

}

RGW_ObjMeta::~RGW_ObjMeta() {

}

void RGW_ObjMeta::insert_to_metamap(const std::string& metaname, const bufferlist& metavalue) {
  ldout(cct, 0) << __func__ << " " << metaname << " has set into combine_meta." << dendl;
  combine_meta.insert(pair<std::string, bufferlist>(metaname, metavalue));
}

void RGW_ObjMeta::insert_to_kv(const std::string& objkey, const bufferlist& combine_metavalue) {
  clear_objmeta_kv();
  objmeta_kv.insert(pair<std::string, bufferlist>(objkey, combine_metavalue));
  /*map<string, bufferlist> kv_map = objmeta_kv; //TODO:should add lock ??
  operateKV->queue(kv_map);*/
  clear_combine_meta();
  ldout(cct, 0) << __func__ << " " << objkey << " has set into kv_map." << dendl;
}
