#include "_cgo_export.h"

unsigned char* mallocUChar(int size) {
	unsigned char* str = (unsigned char*)malloc(sizeof(unsigned char) * size);
	memset(str, 0, sizeof(unsigned char) * size);
	return str;
}

void copyUChar(unsigned char* str, unsigned char* v, int len) {
	memcpy(str, v, len);
}

KV_return** mallocKVStruct(int limit) {
        KV_return** result = (KV_return**)malloc(sizeof(KV_return*) * limit);
        int i;
        for (i = 0; i < limit; ++i) {
            result[i] = (KV_return*)malloc(sizeof(KV_return));
            result[i]->k = (unsigned char*)malloc(sizeof(unsigned char) * 1024);
            result[i]->v = (unsigned char*)malloc(sizeof(unsigned char) * 102400);
			memset(result[i]->k, 0, sizeof(unsigned char) * 1024);
			memset(result[i]->v, 0, sizeof(unsigned char) * 102400);
        }
        return result;
}

void copyKVStruct(KV_return** kv_return, const unsigned char* k, const unsigned char* v, int index, int klen, int vlen) {
    memcpy(kv_return[index]->k, k, klen);
    memcpy(kv_return[index]->v, v, vlen);
	kv_return[index]->klen = klen;
	kv_return[index]->vlen = vlen;
	//printf("XXXXXX: kv_return->k: %s kv_return->v: %s\n", kv_return[index]->k, kv_return[index]->v);
}

void FreeKVStruct(KV_return** kv_return, int limit){
        if(kv_return != 0){
                int i;
                for (i = 0; i < limit; ++i) {
                        if(kv_return[i]->k != 0) {
                                free(kv_return[i]->k);
                                printf("free %i k\n", i);
                        }                                    
                        if(kv_return[i]->v != 0) {
                                free(kv_return[i]->v);
                                printf("free %i v\n", i);
                        }
                        free(kv_return[i]);
                        printf("free kv_retrun\n");
                }
        }
}

char* getKVStructKey(KV_return** kv, int index) {
	return kv[index]->k;
}

char* getKVStructVal(KV_return** kv, int index) {
	return kv[index]->v;
}

char* getKeys(char** keys, int index) {
	return keys[index];
}
