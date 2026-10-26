/* GDSF_S3FIFO_shared / GDSF_S3FIFO_rekey / GDSF_S3FIFO_warmup
 *
 * One cache (one hash table, one copy of every object) whose eviction policy
 * switches between GDSF and S3FIFO. The three variants differ in how the
 * metadata of the policy that is not evicting is kept:
 *
 *  shared: the metadata of both policies is updated on every request, a switch
 *          only changes which policy evicts.
 *  rekey:  only the active policy's metadata is kept, a switch derives the
 *          incoming policy's metadata from it (GDSF -> S3FIFO: the highest
 *          priority objects fill the main queue, the rest go to the small queue;
 *          S3FIFO -> GDSF: the S3FIFO frequency becomes the GDSF frequency).
 *  warmup: like rekey, but the incoming policy's metadata is derived
 *          warmup-window requests before the switch and updated from then on,
 *          so it has observed that many requests when it takes over.
 *          warmup-window=0 is the same as rekey.
 *
 * The cache switches policy every switch-interval requests (counting every
 * get), the switch runs inside get so its cost is part of that request.
 * GDSF_S3FIFO_*_switch switches immediately instead (warmup: after
 * warmup-window more requests).
 *
 * The S3FIFO side follows S3FIFO.c: a 10% small FIFO, a 90% main FIFO with a
 * frequency capped at 3, and a ghost FIFO of the objects evicted from the
 * small FIFO.
 */

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <set>
#include <unordered_map>

#include "dataStructure/hashtable/hashtable.h"
#include "libCacheSim/evictionAlgo.h"

namespace {

/* the segment length of data/switch.oracleGeneral.zst, so that a default run
 * switches at its phase boundaries; switch-interval=0 never switches */
constexpr int64_t kDefaultSwitchInterval = 4229235;

enum class Policy : uint8_t { GDSF, S3FIFO };
enum class Variant : uint8_t { Shared, Rekey, Warmup };

static Policy other(Policy policy) {
  return policy == Policy::GDSF ? Policy::S3FIFO : Policy::GDSF;
}

static inline GDSF_S3FIFO_obj_metadata_t &meta(cache_obj_t *obj) {
  return obj->GDSF_S3FIFO;
}

struct GdsfKey {
  double priority;
  int64_t seq;
  cache_obj_t *obj;
};

struct GdsfKeyLess {
  bool operator()(const GdsfKey &left, const GdsfKey &right) const {
    if (left.priority != right.priority) return left.priority < right.priority;
    if (left.seq != right.seq) return left.seq < right.seq;
    return left.obj < right.obj;
  }
};

static GdsfKey gdsf_key(cache_obj_t *obj) {
  return {meta(obj).gdsf_priority, meta(obj).gdsf_seq, obj};
}

/* intrusive FIFO through cache_obj.queue, head is the newest, tail the oldest */
struct Fifo {
  cache_obj_t *head = nullptr;
  cache_obj_t *tail = nullptr;
  int64_t bytes = 0;

  void push_head(cache_obj_t *obj) {
    obj->queue.prev = nullptr;
    obj->queue.next = head;
    if (head != nullptr) {
      head->queue.prev = obj;
    } else {
      tail = obj;
    }
    head = obj;
    bytes += obj->obj_size;
  }

  void unlink(cache_obj_t *obj) {
    if (obj->queue.prev != nullptr) {
      obj->queue.prev->queue.next = obj->queue.next;
    } else {
      head = obj->queue.next;
    }
    if (obj->queue.next != nullptr) {
      obj->queue.next->queue.prev = obj->queue.prev;
    } else {
      tail = obj->queue.prev;
    }
    obj->queue.prev = obj->queue.next = nullptr;
    bytes -= obj->obj_size;
  }
};

struct GhostEntry {
  obj_id_t id;
  int64_t size;
  uint64_t generation;
};

struct GhostRecord {
  int64_t size;
  uint64_t generation;
};

struct HybridState {
  Variant variant = Variant::Shared;
  Policy active = Policy::GDSF;
  bool track_gdsf = false;
  bool track_s3fifo = false;

  int64_t switch_interval = kDefaultSwitchInterval;
  int64_t warmup_window = 0;
  int64_t n_get = 0;
  /* a warmup switch requested through GDSF_S3FIFO_warmup_switch commits when
   * n_get reaches this */
  int64_t pending_commit_at = -1;
  Policy pending_policy = Policy::GDSF;

  std::set<GdsfKey, GdsfKeyLess> gdsf_queue;
  double gdsf_last_evict = 0;
  int64_t next_seq = 0;

  Fifo small;
  Fifo main;
  int64_t small_capacity = 0;
  int64_t main_capacity = 0;
  double small_size_ratio = 0.10;
  double ghost_size_ratio = 0.90;
  int move_to_main_threshold = 2;
  int freq_cap = 3;
  bool has_evicted = false;
  bool hit_on_ghost = false;

  std::deque<GhostEntry> ghost_order;
  std::unordered_map<obj_id_t, GhostRecord> ghosts;
  uint64_t next_ghost_generation = 0;
  int64_t ghost_bytes = 0;
  int64_t ghost_capacity = 0;
};

static HybridState *get_state(const cache_t *cache) {
  return static_cast<HybridState *>(cache->eviction_params);
}

/* ############################## ghost ############################## */

/* same as inserting into the ghost FIFO of S3FIFO.c: an id already in the
 * ghost keeps its position */
static void ghost_add(HybridState *state, obj_id_t id, int64_t size) {
  if (size > state->ghost_capacity || state->ghosts.count(id) != 0) return;

  while (state->ghost_bytes + size > state->ghost_capacity) {
    GhostEntry oldest = state->ghost_order.front();
    state->ghost_order.pop_front();
    auto current = state->ghosts.find(oldest.id);
    if (current != state->ghosts.end() &&
        current->second.generation == oldest.generation) {
      state->ghosts.erase(current);
      state->ghost_bytes -= oldest.size;
    }
  }

  uint64_t generation = ++state->next_ghost_generation;
  state->ghosts[id] = {size, generation};
  state->ghost_order.push_back({id, size, generation});
  state->ghost_bytes += size;
}

static bool ghost_remove(HybridState *state, obj_id_t id) {
  auto ghost = state->ghosts.find(id);
  if (ghost == state->ghosts.end()) return false;
  state->ghost_bytes -= ghost->second.size;
  state->ghosts.erase(ghost);
  return true;
}

static void ghost_clear(HybridState *state) {
  state->ghost_order.clear();
  state->ghosts.clear();
  state->ghost_bytes = 0;
}

/* ############################## GDSF ############################## */

static void gdsf_set_priority(HybridState *state, cache_obj_t *obj) {
  meta(obj).gdsf_priority = state->gdsf_last_evict +
                            (double)meta(obj).gdsf_freq * 1.0e6 / obj->obj_size;
  meta(obj).gdsf_seq = ++state->next_seq;
}

static void gdsf_add(HybridState *state, cache_obj_t *obj) {
  state->gdsf_queue.insert(gdsf_key(obj));
  meta(obj).in_gdsf = true;
}

static void gdsf_erase(HybridState *state, cache_obj_t *obj) {
  state->gdsf_queue.erase(gdsf_key(obj));
  meta(obj).in_gdsf = false;
}

static void gdsf_on_hit(HybridState *state, cache_obj_t *obj) {
  gdsf_erase(state, obj);
  meta(obj).gdsf_freq += 1;
  gdsf_set_priority(state, obj);
  gdsf_add(state, obj);
}

static void gdsf_on_insert(HybridState *state, cache_obj_t *obj) {
  meta(obj).gdsf_freq = 1;
  gdsf_set_priority(state, obj);
  gdsf_add(state, obj);
}

/* ############################## S3FIFO ############################## */

static void s3fifo_add(HybridState *state, cache_obj_t *obj, bool to_main) {
  meta(obj).in_s3fifo = true;
  meta(obj).in_main = to_main;
  (to_main ? state->main : state->small).push_head(obj);
}

static void s3fifo_erase(HybridState *state, cache_obj_t *obj) {
  (meta(obj).in_main ? state->main : state->small).unlink(obj);
  meta(obj).in_s3fifo = false;
}

static void s3fifo_on_hit(HybridState *state, cache_obj_t *obj) {
  /* S3FIFO.c counts without a bound but only compares the small queue's count
   * with move-to-main-threshold and uses MIN(freq, 3) in the main queue */
  if (meta(obj).s3fifo_freq < state->freq_cap) meta(obj).s3fifo_freq += 1;
}

static void s3fifo_on_insert(HybridState *state, cache_obj_t *obj) {
  bool to_main = state->hit_on_ghost ||
                 obj->obj_size >= state->small_capacity ||
                 (!state->has_evicted &&
                  state->small.bytes >= state->small_capacity);
  meta(obj).s3fifo_freq = 0;
  s3fifo_add(state, obj, to_main);
}

/* ############################## switching ############################## */

/* GDSF -> S3FIFO: the highest priority objects fill the main queue in priority
 * order (the highest is the newest), the rest go to the small queue */
static void build_s3fifo_from_gdsf(HybridState *state) {
  DEBUG_ASSERT(state->track_gdsf && !state->track_s3fifo);
  state->small = Fifo{};
  state->main = Fifo{};
  ghost_clear(state);

  int64_t main_bytes = 0;
  bool main_full = false;
  for (auto it = state->gdsf_queue.rbegin(); it != state->gdsf_queue.rend();
       ++it) {
    cache_obj_t *obj = it->obj;
    if (!main_full && main_bytes + obj->obj_size <= state->main_capacity) {
      meta(obj).in_main = true;
      main_bytes += obj->obj_size;
    } else {
      main_full = true;
      meta(obj).in_main = false;
    }
  }

  for (const GdsfKey &key : state->gdsf_queue) {
    cache_obj_t *obj = key.obj;
    meta(obj).s3fifo_freq =
        (int8_t)std::min(meta(obj).gdsf_freq - 1, state->freq_cap);
    s3fifo_add(state, obj, meta(obj).in_main);
  }
  state->track_s3fifo = true;
}

/* S3FIFO -> GDSF: the S3FIFO frequency becomes the GDSF frequency, objects
 * closer to the tail of the main queue are the older ones on equal priority */
static void build_gdsf_from_s3fifo(HybridState *state) {
  DEBUG_ASSERT(state->track_s3fifo && !state->track_gdsf);
  state->gdsf_queue.clear();
  for (Fifo *fifo : {&state->main, &state->small}) {
    for (cache_obj_t *obj = fifo->tail; obj != nullptr; obj = obj->queue.prev) {
      meta(obj).gdsf_freq = meta(obj).s3fifo_freq + 1;
      gdsf_set_priority(state, obj);
      gdsf_add(state, obj);
    }
  }
  state->track_gdsf = true;
}

static void start_tracking(HybridState *state, Policy policy) {
  if (policy == Policy::GDSF && !state->track_gdsf) {
    build_gdsf_from_s3fifo(state);
  } else if (policy == Policy::S3FIFO && !state->track_s3fifo) {
    build_s3fifo_from_gdsf(state);
  }
}

static void stop_tracking(HybridState *state, Policy policy) {
  if (policy == Policy::GDSF && state->track_gdsf) {
    for (const GdsfKey &key : state->gdsf_queue) meta(key.obj).in_gdsf = false;
    state->gdsf_queue.clear();
    state->track_gdsf = false;
  } else if (policy == Policy::S3FIFO && state->track_s3fifo) {
    for (Fifo *fifo : {&state->small, &state->main}) {
      for (cache_obj_t *obj = fifo->head; obj != nullptr;
           obj = obj->queue.next) {
        meta(obj).in_s3fifo = false;
      }
      *fifo = Fifo{};
    }
    ghost_clear(state);
    state->hit_on_ghost = false;
    state->track_s3fifo = false;
  }
}

/* make policy the evicting one, rekey and warmup stop updating the other one */
static void commit_switch(HybridState *state, Policy policy) {
  start_tracking(state, policy);
  state->active = policy;
  if (state->variant != Variant::Shared) stop_tracking(state, other(policy));
}

/* ############################## cache_t interface ##########################
 */

static void remove_obj(cache_t *cache, cache_obj_t *obj, bool evicted) {
  HybridState *state = get_state(cache);
  if (meta(obj).in_gdsf) gdsf_erase(state, obj);
  if (meta(obj).in_s3fifo) {
    if (evicted && !meta(obj).in_main) {
      ghost_add(state, obj->obj_id, obj->obj_size);
    }
    s3fifo_erase(state, obj);
  }
  cache_remove_obj_base(cache, obj, true);
}

static void evict_gdsf(cache_t *cache) {
  HybridState *state = get_state(cache);
  DEBUG_ASSERT(!state->gdsf_queue.empty());
  auto first = state->gdsf_queue.begin();
  state->gdsf_last_evict = first->priority;
  remove_obj(cache, first->obj, true);
}

/* S3FIFO_evict in S3FIFO.c */
static void evict_s3fifo(cache_t *cache) {
  HybridState *state = get_state(cache);

  if (state->main.bytes > state->main_capacity || state->small.bytes == 0) {
    while (state->main.tail != nullptr) {
      cache_obj_t *obj = state->main.tail;
      if (meta(obj).s3fifo_freq >= 1) {
        meta(obj).s3fifo_freq = (int8_t)(std::min<int>(meta(obj).s3fifo_freq, 3) - 1);
        state->main.unlink(obj);
        state->main.push_head(obj);
      } else {
        remove_obj(cache, obj, true);
        return;
      }
    }
    return;
  }

  while (state->small.tail != nullptr) {
    cache_obj_t *obj = state->small.tail;
    if (meta(obj).s3fifo_freq >= state->move_to_main_threshold) {
      state->small.unlink(obj);
      meta(obj).in_main = true;
      meta(obj).s3fifo_freq = 0;
      state->main.push_head(obj);
    } else {
      remove_obj(cache, obj, true);
      return;
    }
  }
}

static void hybrid_free(cache_t *cache) {
  delete get_state(cache);
  cache_struct_free(cache);
}

static bool hybrid_get(cache_t *cache, const request_t *req) {
  HybridState *state = get_state(cache);
  int64_t idx = state->n_get++;

  if (state->switch_interval > 0 && idx > 0) {
    if (idx % state->switch_interval == 0) {
      commit_switch(state, other(state->active));
    } else if (state->variant == Variant::Warmup && state->warmup_window > 0 &&
               (idx + state->warmup_window) % state->switch_interval == 0) {
      start_tracking(state, other(state->active));
    }
  }
  if (idx == state->pending_commit_at) {
    commit_switch(state, state->pending_policy);
    state->pending_commit_at = -1;
  }

  if (state->active == Policy::S3FIFO) return cache_get_base(cache, req);

  /* GDSF_get in GDSF.cpp: the object is inserted before evicting, so it can be
   * the one evicted */
  cache->n_req += 1;
  bool hit = cache->find(cache, req, true) != nullptr;
  if (!hit && cache->can_insert(cache, req)) {
    cache->insert(cache, req);
    while (cache->get_occupied_byte(cache) > cache->cache_size) {
      cache->evict(cache, req);
    }
  }
  return hit;
}

static cache_obj_t *hybrid_find(cache_t *cache, const request_t *req,
                                bool update_cache) {
  HybridState *state = get_state(cache);
  cache_obj_t *obj = cache_find_base(cache, req, update_cache);
  if (!update_cache) return obj;

  state->hit_on_ghost = false;
  if (obj != nullptr) {
    if (state->track_gdsf) gdsf_on_hit(state, obj);
    if (state->track_s3fifo) s3fifo_on_hit(state, obj);
  } else if (state->track_s3fifo) {
    state->hit_on_ghost = ghost_remove(state, req->obj_id);
  }
  return obj;
}

static cache_obj_t *hybrid_insert(cache_t *cache, const request_t *req) {
  HybridState *state = get_state(cache);
  cache_obj_t *obj = cache_insert_base(cache, req);
  meta(obj).in_gdsf = false;
  meta(obj).in_s3fifo = false;
  if (state->track_gdsf) gdsf_on_insert(state, obj);
  if (state->track_s3fifo) s3fifo_on_insert(state, obj);
  state->hit_on_ghost = false;
  return obj;
}

static void hybrid_evict(cache_t *cache, const request_t *req) {
  (void)req;
  HybridState *state = get_state(cache);
  state->has_evicted = true;
  if (state->active == Policy::GDSF) {
    evict_gdsf(cache);
  } else {
    evict_s3fifo(cache);
  }
}

static bool hybrid_remove(cache_t *cache, obj_id_t obj_id) {
  cache_obj_t *obj = hashtable_find_obj_id(cache->hashtable, obj_id);
  if (obj == nullptr) return false;
  remove_obj(cache, obj, false);
  return true;
}

/* the next object evict removes, ignoring the main queue's reinsertions */
static cache_obj_t *hybrid_to_evict(cache_t *cache, const request_t *req) {
  (void)req;
  HybridState *state = get_state(cache);
  if (state->active == Policy::GDSF) {
    return state->gdsf_queue.empty() ? nullptr : state->gdsf_queue.begin()->obj;
  }
  if (state->main.bytes > state->main_capacity || state->small.bytes == 0) {
    return state->main.tail;
  }
  return state->small.tail;
}

/* S3FIFO.c does not insert an object that does not fit the small queue
 * unless it is a ghost hit */
static bool hybrid_can_insert(cache_t *cache, const request_t *req) {
  HybridState *state = get_state(cache);
  if (!cache_can_insert_default(cache, req)) return false;
  return state->active != Policy::S3FIFO || state->hit_on_ghost ||
         req->obj_size < state->small_capacity;
}

/* ############################## init ############################## */

static Policy parse_policy(const char *policy_name) {
  if (strcasecmp(policy_name, "gdsf") == 0) return Policy::GDSF;
  if (strcasecmp(policy_name, "s3fifo") == 0) return Policy::S3FIFO;
  ERROR("GDSF_S3FIFO: unknown policy %s, use gdsf or s3fifo\n", policy_name);
  exit(1);
}

static void parse_params(HybridState *state, const char *params,
                         const char *cache_name) {
  if (params == nullptr) return;
  char *copy = strdup(params);
  char *cursor = copy;
  while (cursor != nullptr && *cursor != '\0') {
    char *key = strsep(&cursor, "=");
    char *value = strsep(&cursor, ",");
    while (cursor != nullptr && *cursor == ' ') cursor++;
    if (strcasecmp(key, "print") == 0) {
      printf("parameters: small-size-ratio=%.4lf,ghost-size-ratio=%.4lf,"
             "move-to-main-threshold=%d,switch-interval=%ld,init-policy=%s",
             state->small_size_ratio, state->ghost_size_ratio,
             state->move_to_main_threshold, (long)state->switch_interval,
             state->active == Policy::GDSF ? "gdsf" : "s3fifo");
      if (state->variant == Variant::Warmup) {
        printf(",warmup-window=%ld", (long)state->warmup_window);
      }
      printf("\n");
      free(copy);
      exit(0);
    }
    if (value == nullptr) {
      ERROR("%s parameter %s has no value\n", cache_name, key);
      exit(1);
    }
    if (strcasecmp(key, "small-size-ratio") == 0 ||
        strcasecmp(key, "fifo-size-ratio") == 0) {
      state->small_size_ratio = strtod(value, nullptr);
    } else if (strcasecmp(key, "ghost-size-ratio") == 0) {
      state->ghost_size_ratio = strtod(value, nullptr);
    } else if (strcasecmp(key, "move-to-main-threshold") == 0) {
      state->move_to_main_threshold = atoi(value);
    } else if (strcasecmp(key, "switch-interval") == 0) {
      state->switch_interval = atoll(value);
    } else if (strcasecmp(key, "init-policy") == 0) {
      state->active = parse_policy(value);
    } else if (strcasecmp(key, "warmup-window") == 0 &&
               state->variant == Variant::Warmup) {
      state->warmup_window = atoll(value);
    } else {
      ERROR("%s does not have parameter %s\n", cache_name, key);
      exit(1);
    }
  }
  free(copy);
}

static cache_t *hybrid_init(const common_cache_params_t ccache_params,
                            const char *cache_specific_params, Variant variant,
                            const char *cache_name,
                            cache_t *(*init_func)(const common_cache_params_t,
                                                  const char *)) {
  cache_t *cache =
      cache_struct_init(cache_name, ccache_params, cache_specific_params);
  HybridState *state = new HybridState;
  state->variant = variant;
  parse_params(state, cache_specific_params, cache_name);

  if (state->move_to_main_threshold < 1 || state->move_to_main_threshold > 100) {
    ERROR("%s move-to-main-threshold must be in [1, 100]\n", cache_name);
    exit(1);
  }
  if (state->switch_interval < 0 || state->warmup_window < 0 ||
      (state->switch_interval > 0 &&
       state->warmup_window >= state->switch_interval)) {
    ERROR("%s needs 0 <= warmup-window < switch-interval\n", cache_name);
    exit(1);
  }
  state->freq_cap = std::max(3, state->move_to_main_threshold);
  state->small_capacity =
      (int64_t)(ccache_params.cache_size * state->small_size_ratio);
  state->main_capacity = ccache_params.cache_size - state->small_capacity;
  state->ghost_capacity =
      (int64_t)(ccache_params.cache_size * state->ghost_size_ratio);
  state->track_gdsf =
      variant == Variant::Shared || state->active == Policy::GDSF;
  state->track_s3fifo =
      variant == Variant::Shared || state->active == Policy::S3FIFO;

  cache->eviction_params = state;
  cache->cache_init = init_func;
  cache->cache_free = hybrid_free;
  cache->get = hybrid_get;
  cache->find = hybrid_find;
  cache->insert = hybrid_insert;
  cache->evict = hybrid_evict;
  cache->remove = hybrid_remove;
  cache->to_evict = hybrid_to_evict;
  cache->get_occupied_byte = cache_get_occupied_byte_default;
  cache->get_n_obj = cache_get_n_obj_default;
  cache->can_insert = hybrid_can_insert;
  cache->obj_md_size = 0;
  if (variant == Variant::Warmup) {
    snprintf(cache->cache_name, CACHE_NAME_ARRAY_LEN, "%s-%ld", cache_name,
             (long)state->warmup_window);
  }
  return cache;
}

static bool hybrid_switch(cache_t *cache, const char *policy_name) {
  HybridState *state = get_state(cache);
  Policy policy;
  if (strcasecmp(policy_name, "gdsf") == 0) {
    policy = Policy::GDSF;
  } else if (strcasecmp(policy_name, "s3fifo") == 0) {
    policy = Policy::S3FIFO;
  } else {
    return false;
  }

  state->pending_commit_at = -1;
  if (state->variant == Variant::Warmup && state->warmup_window > 0 &&
      policy != state->active) {
    start_tracking(state, policy);
    state->pending_policy = policy;
    state->pending_commit_at = state->n_get + state->warmup_window;
  } else {
    commit_switch(state, policy);
  }
  return true;
}

}  // namespace

extern "C" {

cache_t *GDSF_S3FIFO_shared_init(const common_cache_params_t ccache_params,
                                 const char *cache_specific_params) {
  return hybrid_init(ccache_params, cache_specific_params, Variant::Shared,
                     "GDSF_S3FIFO_shared", GDSF_S3FIFO_shared_init);
}

bool GDSF_S3FIFO_shared_switch(cache_t *cache, const char *policy_name) {
  if (cache == nullptr || policy_name == nullptr ||
      cache->cache_init != GDSF_S3FIFO_shared_init) {
    return false;
  }
  return hybrid_switch(cache, policy_name);
}

cache_t *GDSF_S3FIFO_rekey_init(const common_cache_params_t ccache_params,
                                const char *cache_specific_params) {
  return hybrid_init(ccache_params, cache_specific_params, Variant::Rekey,
                     "GDSF_S3FIFO_rekey", GDSF_S3FIFO_rekey_init);
}

bool GDSF_S3FIFO_rekey_switch(cache_t *cache, const char *policy_name) {
  if (cache == nullptr || policy_name == nullptr ||
      cache->cache_init != GDSF_S3FIFO_rekey_init) {
    return false;
  }
  return hybrid_switch(cache, policy_name);
}

cache_t *GDSF_S3FIFO_warmup_init(const common_cache_params_t ccache_params,
                                 const char *cache_specific_params) {
  return hybrid_init(ccache_params, cache_specific_params, Variant::Warmup,
                     "GDSF_S3FIFO_warmup", GDSF_S3FIFO_warmup_init);
}

bool GDSF_S3FIFO_warmup_switch(cache_t *cache, const char *policy_name) {
  if (cache == nullptr || policy_name == nullptr ||
      cache->cache_init != GDSF_S3FIFO_warmup_init) {
    return false;
  }
  return hybrid_switch(cache, policy_name);
}

}  // extern "C"
