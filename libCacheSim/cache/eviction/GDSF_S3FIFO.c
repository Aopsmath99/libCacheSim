/* GDSF_S3FIFO: switch between an unmodified GDSF and an unmodified S3FIFO
 *
 * Only one of the two caches exists at a time and a switch replaces it with an
 * empty cache of the other policy, so the cached data is lost on every switch.
 * This is the baseline for GDSF_S3FIFO_shared/rekey/warmup, which keep the
 * cached objects across a switch.
 *
 * The cache switches every switch-interval requests (counting every get), the
 * switch runs inside get so its cost is part of that request.
 * GDSF_S3FIFO_switch switches immediately instead. The parameters other than
 * switch-interval and init-policy are passed to S3FIFO.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "libCacheSim/evictionAlgo.h"

/* the segment length of data/switch.oracleGeneral.zst, so that a default run
 * switches at its phase boundaries; switch-interval=0 never switches */
#define GDSF_S3FIFO_DEFAULT_SWITCH_INTERVAL 4229235

typedef struct {
  cache_t *active;
  bool active_is_s3fifo;
  int64_t switch_interval;
  int64_t n_get;
  common_cache_params_t ccache_params;
  char s3fifo_params[CACHE_INIT_PARAMS_LEN];
} GDSF_S3FIFO_params_t;

static void GDSF_S3FIFO_parse_params(cache_t *cache,
                                     const char *cache_specific_params);

static cache_t *GDSF_S3FIFO_active(const cache_t *cache) {
  return ((GDSF_S3FIFO_params_t *)cache->eviction_params)->active;
}

/* replace the active cache with an empty cache of the given policy */
static void GDSF_S3FIFO_activate(cache_t *cache, bool s3fifo) {
  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)cache->eviction_params;
  if (params->active != NULL) {
    params->active->cache_free(params->active);
  }
  params->active_is_s3fifo = s3fifo;
  params->active =
      s3fifo ? S3FIFO_init(params->ccache_params,
                           params->s3fifo_params[0] == '\0'
                               ? NULL
                               : params->s3fifo_params)
             : GDSF_init(params->ccache_params, NULL);
  cache->obj_md_size = params->active->obj_md_size;
}

static void GDSF_S3FIFO_free(cache_t *cache) {
  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)cache->eviction_params;
  params->active->cache_free(params->active);
  free(params);
  cache_struct_free(cache);
}

static bool GDSF_S3FIFO_get(cache_t *cache, const request_t *req) {
  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)cache->eviction_params;
  int64_t idx = params->n_get++;
  if (params->switch_interval > 0 && idx > 0 &&
      idx % params->switch_interval == 0) {
    GDSF_S3FIFO_activate(cache, !params->active_is_s3fifo);
  }
  return params->active->get(params->active, req);
}

static cache_obj_t *GDSF_S3FIFO_find(cache_t *cache, const request_t *req,
                                     bool update_cache) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->find(active, req, update_cache);
}

static cache_obj_t *GDSF_S3FIFO_insert(cache_t *cache, const request_t *req) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->insert(active, req);
}

static void GDSF_S3FIFO_evict(cache_t *cache, const request_t *req) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  active->evict(active, req);
}

static bool GDSF_S3FIFO_remove(cache_t *cache, obj_id_t obj_id) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->remove(active, obj_id);
}

static cache_obj_t *GDSF_S3FIFO_to_evict(cache_t *cache,
                                         const request_t *req) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->to_evict == NULL ? NULL : active->to_evict(active, req);
}

static int64_t GDSF_S3FIFO_get_occupied_byte(const cache_t *cache) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->get_occupied_byte(active);
}

static int64_t GDSF_S3FIFO_get_n_obj(const cache_t *cache) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->get_n_obj(active);
}

static bool GDSF_S3FIFO_can_insert(cache_t *cache, const request_t *req) {
  cache_t *active = GDSF_S3FIFO_active(cache);
  return active->can_insert(active, req);
}

cache_t *GDSF_S3FIFO_init(const common_cache_params_t ccache_params,
                          const char *cache_specific_params) {
  cache_t *cache = cache_struct_init_without_hashtable(
      "GDSF_S3FIFO", ccache_params, cache_specific_params);
  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)calloc(1, sizeof(GDSF_S3FIFO_params_t));
  params->switch_interval = GDSF_S3FIFO_DEFAULT_SWITCH_INTERVAL;
  params->ccache_params = ccache_params;
  cache->eviction_params = params;
  GDSF_S3FIFO_parse_params(cache, cache_specific_params);
  GDSF_S3FIFO_activate(cache, params->active_is_s3fifo);

  cache->cache_init = GDSF_S3FIFO_init;
  cache->cache_free = GDSF_S3FIFO_free;
  cache->get = GDSF_S3FIFO_get;
  cache->find = GDSF_S3FIFO_find;
  cache->insert = GDSF_S3FIFO_insert;
  cache->evict = GDSF_S3FIFO_evict;
  cache->remove = GDSF_S3FIFO_remove;
  cache->to_evict = GDSF_S3FIFO_to_evict;
  cache->get_occupied_byte = GDSF_S3FIFO_get_occupied_byte;
  cache->get_n_obj = GDSF_S3FIFO_get_n_obj;
  cache->can_insert = GDSF_S3FIFO_can_insert;

  return cache;
}

bool GDSF_S3FIFO_switch(cache_t *cache, const char *policy_name) {
  if (cache == NULL || policy_name == NULL ||
      cache->cache_init != GDSF_S3FIFO_init) {
    return false;
  }

  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)cache->eviction_params;
  bool s3fifo;
  if (strcasecmp(policy_name, "gdsf") == 0) {
    s3fifo = false;
  } else if (strcasecmp(policy_name, "s3fifo") == 0) {
    s3fifo = true;
  } else {
    return false;
  }

  if (s3fifo != params->active_is_s3fifo) {
    GDSF_S3FIFO_activate(cache, s3fifo);
  }
  return true;
}

static void GDSF_S3FIFO_parse_params(cache_t *cache,
                                     const char *cache_specific_params) {
  GDSF_S3FIFO_params_t *params =
      (GDSF_S3FIFO_params_t *)cache->eviction_params;
  if (cache_specific_params == NULL) return;

  char *params_str = strdup(cache_specific_params);
  char *cursor = params_str;
  size_t s3fifo_len = 0;
  while (cursor != NULL && *cursor != '\0') {
    while (*cursor == ' ') cursor++;
    char *pair = strsep(&cursor, ",");
    char *eq = strchr(pair, '=');
    size_t key_len = eq == NULL ? strlen(pair) : (size_t)(eq - pair);

    if (eq != NULL && key_len == strlen("switch-interval") &&
        strncasecmp(pair, "switch-interval", key_len) == 0) {
      params->switch_interval = atoll(eq + 1);
      if (params->switch_interval < 0) {
        ERROR("GDSF_S3FIFO switch-interval must be >= 0\n");
        exit(1);
      }
    } else if (eq != NULL && key_len == strlen("init-policy") &&
               strncasecmp(pair, "init-policy", key_len) == 0) {
      if (strcasecmp(eq + 1, "gdsf") == 0) {
        params->active_is_s3fifo = false;
      } else if (strcasecmp(eq + 1, "s3fifo") == 0) {
        params->active_is_s3fifo = true;
      } else {
        ERROR("GDSF_S3FIFO: unknown policy %s, use gdsf or s3fifo\n", eq + 1);
        exit(1);
      }
    } else {
      /* S3FIFO parameter, S3FIFO_init reports the unknown ones */
      int n = snprintf(params->s3fifo_params + s3fifo_len,
                       sizeof(params->s3fifo_params) - s3fifo_len, "%s%s",
                       s3fifo_len == 0 ? "" : ",", pair);
      s3fifo_len += n;
    }
  }
  free(params_str);
}
