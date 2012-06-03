
#ifndef __BPOOL_H__
#define __BPOOL_H__

#define __USE_UNIX98 /* for pthread.h */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include <hashtable.h>

/* G_ASSERT */
#define SOFT_ASSERT 0
#define HARD_ASSERT 1
#define G_ASSERT_ALWAYS 0
#define G_ASSERT(COND,MESG,TYPE) TYPE ? assert(COND) : fprintf(stderr, "FAILED ASSERT: %s\n", MESG)

#define BPOOL_MAX_PARTITIONS 2
#undef BPOOL_ENABLE_LOCKS

typedef unsigned char bbyte;

typedef enum bpool_bool {
  BPOOL_TRUE=0,
  BPOOL_FALSE=1
} bpool_bool;

typedef enum bpool_action {
  BPOOL_READ=0,
  BPOOL_WRITE
} bpool_action;

/* Type of meta-data */
typedef enum bpool_meta_type {
  BPOOL_KEY,
  BPOOL_BUF
} bpool_meta_type;

typedef enum bpool_result {
  CACHE_HIT=0, 
  CACHE_MISS, 
  CACHE_FILL, 
  CACHE_FLUSH, 
  RES_MAX
} bpool_result;

typedef struct bpool_node {
  /* bits */
  bbyte valid;
  bbyte refbit;
  bbyte dirty;
  /* partitions */
  bbyte part_id;
  /* page contents */
  char *id; /* key */
  char *buf; /* value */
#ifdef BPOOL_ENABLE_LOCKS
  /* lock for this record */
  pthread_mutex_t page_lock;
#endif
} bpool_node;


typedef struct bpool_ops {
  int           (*fill)    (void *env, bpool_node *node, int *ret, void *tag);
  int           (*flush)   (void *env, bpool_node *node, int *ret, void *tag);
  int           (*demote)  (void *env, bpool_node *node, int *ret, void *tag);
  int           (*print)   (void *env, void *key, struct bpool_node *node);
  int           (*compare) (struct hashtab *h, void *key1, void *key2);
  unsigned int  (*hash)    (struct hashtab *h, void *key);
  int           (*mset)    (void *env, unsigned type, char *bpool_buf);
  int           (*mread)   (void *env, unsigned type, char *bpool_buf, char *user_buf);
  int           (*mwrite)  (void *env, unsigned type, char *bpool_buf, char *user_buf);
  char*         (*malloc)  (void *env, unsigned type);
  void          (*mfree)   (void *env, unsigned type, void *ptr);
  void          (*stat)    (void *env, int rd_wr, unsigned diff, int result, void *tag);
  unsigned      (*part)    (void *env, void *tag, char *key);
} bpool_ops;

typedef enum bpool_stat_type {
  BPOOL_STAT_READ=0,
  BPOOL_STAT_WRITE,
  BPOOL_STAT_MAX /* must be last */
} bpool_stat_type;

typedef struct bpool_stats {

  /* Global */
  unsigned global_num_gets[BPOOL_STAT_MAX];
  unsigned global_num_hits[BPOOL_STAT_MAX];
  unsigned global_num_misses[BPOOL_STAT_MAX];
  unsigned global_num_fills[BPOOL_STAT_MAX];
  unsigned global_num_flushes[BPOOL_STAT_MAX];
  unsigned global_num_evicts[BPOOL_STAT_MAX];
  unsigned global_num_victims[BPOOL_STAT_MAX];

  /* Latencies */
  unsigned global_lat_gets[BPOOL_STAT_MAX];
  unsigned global_lat_hits[BPOOL_STAT_MAX];
  unsigned global_lat_misses[BPOOL_STAT_MAX];
  unsigned global_lat_fills[BPOOL_STAT_MAX];
  unsigned global_lat_flushes[BPOOL_STAT_MAX];
  unsigned global_lat_evicts[BPOOL_STAT_MAX];
  unsigned global_lat_victims[BPOOL_STAT_MAX];

  /* For Partitions */
  unsigned part_num_gets[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_hits[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_misses[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_fills[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_flushes[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_evicts[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_num_victims[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];

  unsigned part_lat_gets[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_hits[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_misses[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_fills[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_flushes[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_evicts[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];
  unsigned part_lat_victims[BPOOL_MAX_PARTITIONS][BPOOL_STAT_MAX];

} bpool_stats;

typedef struct bpool {
  /* set once */
  void *environment;
  unsigned num_blocks;
  unsigned num_partitions;
  /* ops */
  bpool_ops ops;
  /* contents */
#ifdef BPOOL_ENABLE_LOCKS
  pthread_mutex_t big_lock; /* locks bpool -- use sparingly */
#endif
  unsigned *clock_hand;
  struct hashtab *table;
  struct bpool_node *clock;
  /* counts */
  unsigned  global_size;
  unsigned *part_sizes;
  unsigned *part_quotas;
  /* stats */
#ifdef BPOOL_ENABLE_LOCKS
  pthread_mutex_t stats_lock;
#endif
  bpool_stats stats;
} bpool;



struct bpool * bpool_create_pool( void *env, bpool_ops ops, unsigned num_blocks, unsigned num_partitions );
int            bpool_read       (struct bpool *pool, char *id, char *buf, void *tag);
int            bpool_write      (struct bpool *pool, char *id, char *buf, void *tag);
int            bpool_delete     (struct bpool *pool, char *id, int write_back);
int            bpool_flush      (struct bpool *pool, int should_discard);
int            bpool_print_stats(struct bpool *pool, FILE *fd);
int            bpool_setquota   (struct bpool *pool, unsigned part_id, unsigned num_blocks );
unsigned       bpool_getquota   (struct bpool *pool, unsigned part_id );

#endif
