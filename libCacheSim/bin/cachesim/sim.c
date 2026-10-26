#include <sys/resource.h>
#include <time.h>

#include "cache_init.h"
#include "libCacheSim/cache.h"
#include "libCacheSim/reader.h"
#include "utils/include/mymath.h"
#include "utils/include/mystr.h"
#include "utils/include/mysys.h"

#ifdef __cplusplus
extern "C" {
#endif

void print_head_requests(request_t *req, uint64_t req_cnt) {
  if (req_cnt < 2) {
    print_request(req);
  }
}

static uint64_t monotonic_time_ns(void) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

/* log-linear latency histogram: values below 2^LAT_SUB_BITS ns have their own
 * bucket, larger values share a bucket with values within 1/2^(LAT_SUB_BITS-1)
 * of them, so a percentile is exact up to 1/2^LAT_SUB_BITS (0.4%) */
#define LAT_SUB_BITS 8
#define LAT_N_BUCKETS ((64 - LAT_SUB_BITS + 2) << (LAT_SUB_BITS - 1))

typedef struct {
  uint64_t count;
  uint64_t buckets[LAT_N_BUCKETS];
} latency_hist_t;

static inline int latency_bucket(uint64_t value) {
  if (value < (1ULL << LAT_SUB_BITS)) return (int)value;
  int shift = 63 - __builtin_clzll(value) - LAT_SUB_BITS + 1;
  return (shift << (LAT_SUB_BITS - 1)) + (int)(value >> shift);
}

/* the midpoint of the values in the bucket */
static double latency_bucket_value(int bucket) {
  if (bucket < (1 << LAT_SUB_BITS)) return bucket;
  int shift = bucket / (1 << (LAT_SUB_BITS - 1)) - 1;
  uint64_t mantissa = bucket - ((uint64_t)shift << (LAT_SUB_BITS - 1));
  uint64_t low = mantissa << shift;
  return (double)low + ((1ULL << shift) - 1) / 2.0;
}

static inline void latency_hist_add(latency_hist_t *hist, uint64_t value) {
  hist->buckets[latency_bucket(value)]++;
  hist->count++;
}

/* nearest-rank percentile */
static double latency_hist_percentile(const latency_hist_t *hist,
                                      double percentile) {
  if (hist->count == 0) return 0;
  uint64_t rank = (uint64_t)ceil(percentile * hist->count);
  if (rank == 0) rank = 1;
  uint64_t seen = 0;
  for (int i = 0; i < LAT_N_BUCKETS; i++) {
    seen += hist->buckets[i];
    if (seen >= rank) return latency_bucket_value(i);
  }
  return latency_bucket_value(LAT_N_BUCKETS - 1);
}

/* the median time measured around nothing, i.e. what reading the clock twice
 * adds to every measured cache get */
static uint64_t timer_overhead_ns(void) {
  latency_hist_t *hist = calloc(1, sizeof(latency_hist_t));
  for (int i = 0; i < 1000000; i++) {
    uint64_t start = monotonic_time_ns();
    latency_hist_add(hist, monotonic_time_ns() - start);
  }
  uint64_t overhead = (uint64_t)latency_hist_percentile(hist, 0.5);
  free(hist);
  return overhead;
}

void simulate(reader_t *reader, cache_t *cache, int report_interval,
              int warmup_sec, char *ofilepath, bool ignore_obj_size,
              bool print_head_req, int hashpower) {
  /* random seed */
  srand(time(NULL));
  set_rand_seed(rand());

  request_t *req = new_request();
  uint64_t req_cnt = 0, miss_cnt = 0;
  uint64_t last_req_cnt = 0, last_miss_cnt = 0;
  uint64_t req_byte = 0, miss_byte = 0;
  double req_cost = 0, miss_cost = 0;
  /* latency of each cache get after the warmup, without the cost of reading
   * the clock */
  latency_hist_t *get_latency = calloc(1, sizeof(latency_hist_t));
  uint64_t timer_overhead = timer_overhead_ns();

  read_one_req(reader, req);
  uint64_t start_ts = (uint64_t)req->clock_time;
  uint64_t last_report_ts = warmup_sec;

  char detailed_cache_name[256];
  generate_cache_name(cache, detailed_cache_name, 256);

  double start_time = -1;
  while (req->valid) {
    if (print_head_req) {
      print_head_requests(req, req_cnt);
    }

    req->clock_time -= start_ts;
    if (req->clock_time <= warmup_sec) {
      cache->get(cache, req);
      read_one_req(reader, req);
      continue;
    } else {
      if (start_time < 0) {
        start_time = gettime();
      }
    }

    req_cnt++;
    req_byte += req->obj_size;
    req_cost += req->obj_cost;
    uint64_t cache_get_start_ns = monotonic_time_ns();
    bool cache_hit = cache->get(cache, req);
    uint64_t cache_get_elapsed_ns = monotonic_time_ns() - cache_get_start_ns;
    latency_hist_add(get_latency, cache_get_elapsed_ns > timer_overhead
                                      ? cache_get_elapsed_ns - timer_overhead
                                      : 0);
    if (cache_hit == false) {
      miss_cnt++;
      miss_byte += req->obj_size;
      miss_cost += req->obj_cost;
    }
    if (req->clock_time - last_report_ts >= (uint64_t)report_interval &&
        req->clock_time != 0) {
      INFO(
          "%s %s %.2lf hour: %lu requests, miss ratio %.4lf, interval miss "
          "ratio "
          "%.4lf\n",
          mybasename(reader->trace_path), detailed_cache_name,
          (double)req->clock_time / 3600, (unsigned long)req_cnt,
          (double)miss_cnt / req_cnt,
          (double)(miss_cnt - last_miss_cnt) / (req_cnt - last_req_cnt));
      last_miss_cnt = miss_cnt;
      last_req_cnt = req_cnt;
      last_report_ts = (int64_t)req->clock_time;
    }

    read_one_req(reader, req);
  }

  double runtime = gettime() - start_time;
  struct rusage resource_usage;
  double peak_rss_mib = 0;
  if (getrusage(RUSAGE_SELF, &resource_usage) == 0) {
#ifdef __APPLE__
    peak_rss_mib = (double)resource_usage.ru_maxrss / (1024.0 * 1024.0);
#else
    peak_rss_mib = (double)resource_usage.ru_maxrss / 1024.0;
#endif
  }
  double p50_cache_get_ns = latency_hist_percentile(get_latency, 0.50);
  double p99_cache_get_ns = latency_hist_percentile(get_latency, 0.99);
  free(get_latency);

  char output_str[1024];
  char size_str[64];
  double miss_ratio = req_cnt > 0 ? (double)miss_cnt / (double)req_cnt : 0.0;
  double byte_miss_ratio =
      req_byte > 0 ? (double)miss_byte / (double)req_byte : 0.0;
  double cost_saving_ratio = 1.0 - (req_cost > 0 ? miss_cost / req_cost : 0.0);

  if (!ignore_obj_size)
    convert_size_to_str(cache->cache_size, size_str, 64);
  else
    snprintf(size_str, sizeof(size_str), "%lld", (long long)cache->cache_size);

  bool show_cost = fabs(1 - cost_saving_ratio - miss_ratio) > 1e-9;

  int n = snprintf(output_str, sizeof(output_str),
                   "%s %s cache size %8s, %16lu req, miss ratio %.4lf, "
                   "p50 read latency %.2lf ns, p99 read latency %.2lf ns, "
                   "peak RSS %.2lf MiB",
                   reader->trace_path, detailed_cache_name, size_str,
                   (unsigned long)req_cnt, miss_ratio, p50_cache_get_ns,
                   p99_cache_get_ns, peak_rss_mib);
  if (!ignore_obj_size)
    n += snprintf(output_str + n, sizeof(output_str) - n,
                  ", byte miss ratio %.4lf", byte_miss_ratio);
  if (show_cost)
    n += snprintf(output_str + n, sizeof(output_str) - n,
                  ", cost saving ratio %.4lf", cost_saving_ratio);
  /* cachesim appends to the same result file across runs, so without this two
   * rows with the same trace, algorithm and cache size are indistinguishable
   * even though a non-default hashpower moves the miss ratio of the policies
   * that draw eviction candidates through the hash mask. Recorded only when it
   * is not the default, so ordinary runs keep the format they have always had,
   * and it is the requested value rather than cache->hashtable->hashpower so
   * that this row and the multi-cache rows in main.c report the same number --
   * several algorithms shrink their own table below what was asked for. */
  if (hashpower != DEFAULT_HASHPOWER)
    n += snprintf(output_str + n, sizeof(output_str) - n, ", hashpower %d",
                  hashpower);
  snprintf(output_str + n, sizeof(output_str) - n, ", throughput %.2lf MQPS\n",
           (double)req_cnt / 1000000.0 / runtime);
  printf("%s", output_str);
  char *output_dir = rindex(ofilepath, '/');
  if (output_dir != NULL) {
    size_t dir_length = output_dir - ofilepath;
    char dir_path[1024];
    snprintf(dir_path, dir_length + 1, "%s", ofilepath);
    create_dir(dir_path);
  }
  FILE *output_file = fopen(ofilepath, "a");
  if (output_file == NULL) {
    ERROR("cannot open file %s %s\n", ofilepath, strerror(errno));
    exit(1);
  }
  fprintf(output_file, "%s", output_str);
  fclose(output_file);

#if defined(TRACK_EVICTION_V_AGE)
  while (cache->get_occupied_byte(cache) > 0) {
    cache->evict(cache, req);
  }

#endif
  free_request(req);
  cache->cache_free(cache);
}

#ifdef __cplusplus
}
#endif
