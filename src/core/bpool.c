/**
 * 
 * BPOOL (Buffer Pool)
 * -------------------
 * by Gokul Soundararajan
 *
 * Inspired from Apache Derby's Buffer Pool with modifications to
 * work with pthreads and in C.
 *
 **/


#include <bpool.h>

#ifndef PTHREAD_MUTEX_ERRORCHECK
#define PTHREAD_MUTEX_ERRORCHECK PTHREAD_MUTEX_ERRORCHECK_NP
#endif

/* Function Prototypes */
/* -------------------------------------------------------------- */
typedef struct timeval btime;
static void __bpool_behave( struct bpool *pool );

/* Lock Management */
/* -------------------------------------------------------------- */
/* Wraps around pthread_locks but allows for better debugging */

#ifdef BPOOL_ENABLE_LOCKS
static int __bpool_lock(unsigned int path, pthread_mutex_t *mutex) {
  int ret = 0;
  ret = pthread_mutex_lock(mutex);
  return ret;
}

static int __bpool_trylock(unsigned int path, pthread_mutex_t *mutex) {
  int ret = 0;
  ret = pthread_mutex_trylock(mutex);
  return ret;
}


static int __bpool_unlock(unsigned int path, pthread_mutex_t *mutex) {
  int ret = 0;
  ret = pthread_mutex_unlock(mutex);
  return ret;
}
#endif

/* Statistics */
/* -------------------------------------------------------------- */

double __bpool_time() {

  struct timeval t;
  gettimeofday( &t, NULL);
  return (t.tv_sec*1000.0 + t.tv_usec/1000.0);

} /* end __bpool_time() */

void __bpool_record_latency(struct bpool *pool, void *tag, int rd_wr, btime *start, btime *stop, int ret) {
#ifdef BPOOL_ENABLE_LOCKS
  pthread_mutex_lock(&(pool->stats_lock));
#endif
  unsigned diff = (stop->tv_sec*1000000 + stop->tv_usec) - (start->tv_sec*1000000 + start->tv_usec);
  pool->ops.stat(pool->environment, rd_wr, diff, ret, tag);
#ifdef BPOOL_ENABLE_LOCKS
  pthread_mutex_unlock(&(pool->stats_lock));
#endif
}


/* Implementation */
/* -------------------------------------------------------------- */

/**
 * bpool_create()
 * --------------
 * creates/initialize buffer pool and returns it
 * NULL on error
 *
 **/

struct bpool * bpool_create_pool( void *environment, bpool_ops ops, unsigned num_blocks, unsigned num_partitions ){

  unsigned erred = BPOOL_FALSE;
  struct bpool *pool = NULL;
  pool = (struct bpool *) malloc(sizeof(struct bpool));
  
  if(pool) {

    /* TODO: Initialize variables in pool */

    /* Simple error checking -- gotta have my callbacks  */
    G_ASSERT(ops.fill,     "ERROR: fill() needs to be defined!\n",    HARD_ASSERT);
    G_ASSERT(ops.flush,    "ERROR: flush() needs to be defined!\n",   HARD_ASSERT);
    G_ASSERT(ops.demote,   "ERROR: demote() needs to be defined!\n",  HARD_ASSERT);
    G_ASSERT(ops.hash,     "ERROR: hash() needs to be defined!\n",    HARD_ASSERT);
    G_ASSERT(ops.compare,  "ERROR: compare() needs to be defined!\n", HARD_ASSERT);
    G_ASSERT(ops.mread,  "ERROR: memread() needs to be defined!\n", HARD_ASSERT);
    G_ASSERT(ops.mset,   "ERROR: memset() needs to be defined!\n",  HARD_ASSERT);
    G_ASSERT(ops.mwrite, "ERROR: memwrite() needs to be defined!\n",HARD_ASSERT);
    G_ASSERT(ops.malloc, "ERROR: memalloc() needs to be defined!\n",HARD_ASSERT);
    G_ASSERT(ops.mfree,  "ERROR: memfree() needs to be defined!\n", HARD_ASSERT);

    /* must be less than MAX_PARTITIONS */
    G_ASSERT( num_partitions < BPOOL_MAX_PARTITIONS, 
	      "ERROR: num_partitions must be less than BPOOL_MAX_PARTITIONS!\n",
	      HARD_ASSERT);

    /* initialize parameters */
    pool->ops = ops;
    pool->environment = environment;
    pool->num_blocks = num_blocks;
    pool->num_partitions = num_partitions;

    /* create internals */
    pool->table = (struct hashtab *) hashtab_create(ops.hash, ops.compare, pool->num_blocks );
    if(!pool->table) {
      fprintf(stderr, 
	      "ERROR: bpool_create_pool(): Can't malloc pool->table!\nAsked for %u\n", 
	      pool->num_blocks);
      erred = BPOOL_TRUE;
      goto init_cleanup;
    }


    fprintf(stdout, "NOTE: sizeof(bpool_node) is %lu\n", sizeof(struct bpool_node) );
    pool->clock = (struct bpool_node *) malloc(pool->num_blocks * sizeof(struct bpool_node));
    if(!pool->clock) {
      fprintf(stderr, 
	      "ERROR: bpool_create_pool(): Can't malloc pool->clock!\n"
	      );
      erred = BPOOL_TRUE;
      goto init_cleanup;
    }

    pool->clock_hand = calloc( pool->num_partitions, sizeof(unsigned) );
    if( !pool->clock_hand ) {
      fprintf(stderr,
	      "ERROR: Could not malloc space for clock_hand!!\n"
	      );
      erred = BPOOL_TRUE;
      goto init_cleanup;
    }
    
    pool->part_sizes = calloc( pool->num_partitions, sizeof(unsigned) );
    if( !pool->part_sizes ) {
      fprintf(stderr,
	      "ERROR: Could not malloc space for quotas!!\n"
	      );
      erred = BPOOL_TRUE;
      goto init_cleanup;
    }

    pool->part_quotas = calloc( pool->num_partitions, sizeof(unsigned) );
    if( !pool->part_quotas ) {
      fprintf(stderr,
	      "ERROR: Could not malloc space for quotas!!\n"
	      );
      erred = BPOOL_TRUE;
      goto init_cleanup;
    }

    /* create the buffers */
    unsigned i = 0;
    for(i=0; i < pool->num_blocks; i++) {
      pool->clock[i].buf = (char *) pool->ops.malloc(pool->environment, BPOOL_BUF);
      pool->clock[i].id  = (char *) pool->ops.malloc(pool->environment, BPOOL_KEY);
      pool->ops.mset(pool->environment, BPOOL_BUF, pool->clock[i].buf);
      pool->ops.mset(pool->environment, BPOOL_KEY, pool->clock[i].id);
    } /* end for i loop */

    /* good all memory allocated with no problems */
    /* setup locks */
#ifdef BPOOL_ENABLE_LOCKS
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr,  PTHREAD_MUTEX_ERRORCHECK);
    assert(0 == pthread_mutex_init(&pool->big_lock, &mattr));
#endif

    for(i=0; i < pool->num_blocks; i++) {
#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == pthread_mutex_init(&(pool->clock[i].page_lock), &mattr));
#endif
      pool->clock[i].valid = 0;
      pool->clock[i].refbit = 0;
      pool->clock[i].part_id = 0;
    }

    for(i=0; i < pool->num_partitions; i++ ) {
      pool->clock_hand[i] = 0;
      pool->part_sizes[i] = 0;
      pool->part_quotas[i] = 0;
      pool->global_size = 0;
    }

    /* initialize internal variables */
    pool->ops.print = ops.print;
    pool->ops.fill = ops.fill;
    pool->ops.flush = ops.flush;
    pool->ops.demote = ops.demote;
    pool->ops.hash = ops.hash;
    pool->ops.compare = ops.compare;
    pool->ops.mset = ops.mset;
    pool->ops.mread = ops.mread;
    pool->ops.mwrite = ops.mwrite;
    pool->ops.malloc = ops.malloc;
    pool->ops.mfree = ops.mfree;
    pool->ops.stat = ops.stat;

    /* initialize stats */    
#ifdef BPOOL_ENABLE_LOCKS
    pthread_mutex_init( &pool->stats_lock, &mattr );
#endif
    {
      unsigned x=0, y=0;
      for(x=0; x < BPOOL_MAX_PARTITIONS; x++) {
	for(y=0; y < BPOOL_STAT_MAX; y++) {
	  pool->stats.part_num_gets[x][y] = 0;
	  pool->stats.part_num_hits[x][y] = 0;
	  pool->stats.part_num_misses[x][y] = 0;
	  pool->stats.part_num_fills[x][y] = 0;
	  pool->stats.part_num_flushes[x][y] = 0;
	  pool->stats.part_num_evicts[x][y] = 0;
	  pool->stats.part_num_victims[x][y] = 0;

	  pool->stats.part_lat_gets[x][y] = 0;
	  pool->stats.part_lat_hits[x][y] = 0;
	  pool->stats.part_lat_misses[x][y] = 0;
	  pool->stats.part_lat_fills[x][y] = 0;
	  pool->stats.part_lat_flushes[x][y] = 0;
	  pool->stats.part_lat_evicts[x][y] = 0;
	  pool->stats.part_lat_victims[x][y] = 0;

	  pool->stats.global_num_gets[y] = 0;
	  pool->stats.global_num_hits[y] = 0;
	  pool->stats.global_num_misses[y] = 0;
	  pool->stats.global_num_fills[y] = 0;
	  pool->stats.global_num_flushes[y] = 0;
	  pool->stats.global_num_evicts[y] = 0;
	  pool->stats.global_num_victims[y] = 0;
	  
	  pool->stats.global_lat_gets[y] = 0;
	  pool->stats.global_lat_hits[y] = 0;
	  pool->stats.global_lat_misses[y] = 0;
	  pool->stats.global_lat_fills[y] = 0;
	  pool->stats.global_lat_flushes[y] = 0;
	  pool->stats.global_lat_evicts[y] = 0;
	  pool->stats.global_lat_victims[y] = 0;
	  
	}
      }



    }

  }

 init_cleanup:
  /* TODO: Better Cleanup */
  if(erred == BPOOL_TRUE) { exit(-1); }

  return pool;
}


/**
 * __bpool_find_free_node()
 * ------------------------
 * Find a free node and return the node
 * Partitions:
 *  - To support partitions, we need to find the part_id of the
 *    current request (call it part_id)
 *  - If size[part_id] >= quota[part_id] then we can only
 *    remove part_id's blocks
 *  - Else, we can evict anyone else's blocks as long
 *    we went by their clock_hand
 *
 **/

static struct bpool_node * __bpool_find_free_node(struct bpool *cache, unsigned int rd_wr, unsigned part_id, void *tag) {

  struct bpool_node *block;
  unsigned out_loops = 0;
  unsigned over_quota = BPOOL_FALSE;
  

  /* Find a free block */
  while(1) {

    out_loops++;
    unsigned dirty = 0;
    unsigned i = 0;

    if(out_loops > 3 * cache->num_blocks ) { 
      fprintf(stdout, "Notice: Too many loops! [out_loops: %d]\n", out_loops ); exit(-1); 
    }

#ifdef BPOOL_ENABLE_LOCKS
    assert(0 == __bpool_lock(4, &cache->big_lock));
#endif

    for(i=0; i < 64; i++) {
      
      /* check quotas */
      if( cache->part_sizes[part_id] >= cache->part_quotas[part_id] ) {
	over_quota = BPOOL_TRUE;
      } else { over_quota = BPOOL_FALSE; }

      /* read block */
      block = &(cache->clock[ cache->clock_hand[part_id] ]);

      //fprintf(stdout, "Info: Clock hand %u\n", cache->clock_hand[part_id] );

      /* fast lock => fails if already locked */
#ifdef BPOOL_ENABLE_LOCKS
      if(0 == __bpool_trylock(5, &(block->page_lock))) {
#else
	if( 1 ) {
#endif
	/* an invalid block was previously removed */
	if(over_quota == BPOOL_FALSE) {
	  if(!block->valid) {
	    cache->ops.mset(cache->environment, BPOOL_KEY, block->id); 
	    cache->ops.mset(cache->environment, BPOOL_BUF, block->buf);
	    cache->clock_hand[part_id] = (cache->clock_hand[part_id] + 1)%(cache->num_blocks);
	    return block;
	  }
	}
	
	if( (over_quota == BPOOL_TRUE && !block->valid) ||
	    (block->valid && block->part_id != part_id) ) {
#ifdef BPOOL_ENABLE_LOCKS
	  assert(0 == __bpool_unlock(5, &(block->page_lock)));
#endif
	  cache->clock_hand[part_id] = (cache->clock_hand[part_id] + 1)%(cache->num_blocks);
	  /*fprintf(stdout, "Notice: Continuing due to me:%u hand:%u valid:%u part_id: %u over_quota: %u\n",
		  part_id, cache->clock_hand[part_id], block->valid, block->part_id, over_quota );
	  */
	  continue;
	}
	
	
	G_ASSERT(block->valid, "ASSERT: block must be valid here!!\n", HARD_ASSERT);

	/* valid, refbit set => set refbit = 0 */
	if(block->refbit) {
	  /* make refbit = 0, return any block with refbit = 0 */
	  block->refbit = 0;
#ifdef BPOOL_ENABLE_LOCKS
	  assert(0 == __bpool_unlock(5, &(block->page_lock)));
#endif
	} 	
	/* valid, !refbit, dirty block => must be cleaned */
	/* since, i came here first, i must clean it */
	else if(block->dirty) {
	  //fprintf(stdout, "Info: break for cleaning\n");
	  dirty = 1;
	  break;
	}
	/* valid, !refbit, clean block, demoted => good for removal */
	else {
	  G_ASSERT(block->valid && !block->refbit && !block->dirty, 
		   "ASSERT: block must be valid/!refbit/!dirty here!!\n", HARD_ASSERT);
	  hashtab_delete(cache->table, (block->id)); /* remove from hashtable */

	  /* update sizes */
	  cache->global_size--;
	  cache->part_sizes[block->part_id]--;

	  block->valid = 0;
	  cache->ops.mset(cache->environment, BPOOL_KEY, block->id); 
	  cache->ops.mset(cache->environment, BPOOL_BUF, block->buf);
	  cache->clock_hand[part_id] = (cache->clock_hand[part_id] + 1)%(cache->num_blocks);

	  /* stats: record evicts/victims */
#ifdef BPOOL_ENABLE_LOCKS
	  __bpool_lock(0, &cache->stats_lock );
#endif
	  cache->stats.global_num_evicts[rd_wr]++;
	  cache->stats.part_num_evicts[part_id][rd_wr]++;
	  cache->stats.global_num_victims[rd_wr]++;
	  cache->stats.part_num_evicts[block->part_id][rd_wr]++;
#ifdef BPOOL_ENABLE_LOCKS
	  __bpool_unlock(0, &cache->stats_lock );
#endif
	  return block;
	}

      } /* end trylock() */ 
      else { /* fprintf(stdout, "Don't evict a locked page!!\n"); */ }
      
      /* make the clock go around */
      cache->clock_hand[part_id] = (cache->clock_hand[part_id] + 1)%(cache->num_blocks);

    } /* end for loop */

#ifdef BPOOL_ENABLE_LOCKS
    /* release big lock */
    assert(0 == __bpool_unlock(4, &cache->big_lock));
#endif

    /* i exited because my block was dirty */
    /* i'm still holding the page_lock for this node */
    if(dirty) {
      int ret;
      assert(block); assert(block->dirty); assert(block->valid);

      double flush_start_time, flush_stop_time;
      flush_start_time = __bpool_time();
      cache->ops.flush(cache->environment, block, &ret, tag);  
      flush_stop_time = __bpool_time();

      block->dirty = 0;
#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == __bpool_unlock(5, &(block->page_lock)));
#endif

      /* stats: record flush */
#ifdef BPOOL_ENABLE_LOCKS
      __bpool_lock(0, &cache->stats_lock );
#endif
      cache->stats.global_num_flushes[rd_wr]++;
      cache->stats.part_num_flushes[part_id][rd_wr]++;
      cache->stats.global_lat_flushes[rd_wr] += (flush_stop_time-flush_start_time);
      cache->stats.part_lat_flushes[part_id][rd_wr] += (flush_stop_time - flush_start_time);

#ifdef BPOOL_ENABLE_LOCKS
      __bpool_unlock(0, &cache->stats_lock );
#endif

    }

  } /* end while loop */

} /* end find_free_node() */

static int __bpool_get(struct bpool *pool, char *id, char *buf, int rw, void *tag) {


  if(pool && id && buf) {

    struct bpool_node *node = NULL;
    unsigned first = 1;
    unsigned part_id = 0;
    
    /* Find the partition id */
    if(pool->ops.part) {
      part_id = pool->ops.part( pool->environment, tag, id );
      if(part_id < 0 || part_id >= pool->num_partitions ) {
	fprintf(stderr, "Warn: part_id was incorrect! I made it 0.\n");
	part_id = 0;
      }
    }

    /* stat: record get */
#ifdef BPOOL_ENABLE_LOCKS
    __bpool_lock(0, &pool->stats_lock );
#endif

    pool->stats.global_num_gets[rw]++;
    pool->stats.part_num_gets[part_id][rw]++;

#ifdef BPOOL_ENABLE_LOCKS
    __bpool_unlock(0, &pool->stats_lock );
#endif

    /* Access the buffer pool */
    while(1) {

      /* grab the pointer to the block */
#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == __bpool_lock(1, &pool->big_lock));
#endif

      node = hashtab_search(pool->table, (void *) id);

#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == __bpool_unlock(1, &pool->big_lock));
#endif

      /* node - cannot be deleted but identity can be changed by the time I lock it */
      if(node) {

#ifdef BPOOL_ENABLE_LOCKS
	assert(0 == __bpool_lock(2, &(node->page_lock)));
#endif

	/* doesn't match what I want */
	if(pool->ops.compare(pool->table, id, node->id) != 0) {
#ifdef BPOOL_ENABLE_LOCKS
	  assert(0 == __bpool_unlock(2, &(node->page_lock)));
#endif
	  first = 0;
	  continue; /* pointer was bad  -- try again */
	} 

	/* yay!! this is what I want */
	else {

	  unsigned old_part_id = node->part_id;
	  node->part_id = part_id;
	  node->refbit = 1;
	  if(rw == BPOOL_READ) {
	    pool->ops.mread(pool->environment, BPOOL_BUF, node->buf, buf);
	  }
	  else { 
	    pool->ops.mwrite(pool->environment, BPOOL_BUF, node->buf, buf);
	    node->dirty = 1; 
	  }

#ifdef BPOOL_ENABLE_LOCKS
	  assert(0 == __bpool_unlock(2, &(node->page_lock)));
#endif
	  /* switched partitions -- then update counts */
	  if(old_part_id != part_id) {
	    G_ASSERT(old_part_id >=0 && old_part_id < pool->num_partitions,
		       "ASSERT: old_part_id is invalid!!", HARD_ASSERT);
#ifdef BPOOL_ENABLE_LOCKS
	    assert(0 == __bpool_lock(3, &(pool->big_lock)));
#endif
	    pool->part_sizes[old_part_id]--;
	    pool->part_sizes[part_id]++;

#ifdef BPOOL_ENABLE_LOCKS
	    assert(0 == __bpool_unlock(3, &(pool->big_lock)));
#endif

	  }

	  /* stat: record hit */
#ifdef BPOOL_ENABLE_LOCKS
	  __bpool_lock(0, &pool->stats_lock );
#endif

	  pool->stats.global_num_hits[rw]++;
	  pool->stats.part_num_hits[part_id][rw]++;

#ifdef BPOOL_ENABLE_LOCKS
	  __bpool_unlock(0, &pool->stats_lock );	  
#endif
	  return CACHE_HIT;
	}

      } /* end if(node) */

      /* too bad, the node is not even in hashtable */
      else {

	/* make sure everyone is behaving correctly */
	__bpool_behave(pool);

	/* searches clock for free node - returns with lock on hashtable and page
	*/
	node = __bpool_find_free_node(pool, rw, part_id, tag);

	if(node) {

	  /* someone else added it in the meantime */
	  if(hashtab_search(pool->table, (void *) id)) {
	    first = 0;
#ifdef BPOOL_ENABLE_LOCKS
	    assert(0 == __bpool_unlock(4, &pool->big_lock));
	    assert(0 == __bpool_unlock(5, &node->page_lock));
#endif
	    continue;
	  } 

	  /*else*/
	  {
#ifdef BPOOL_ENABLE_LOCKS
	    __bpool_lock(0, &pool->stats_lock );
#endif

	    pool->stats.global_num_misses[rw]++;
	    pool->stats.part_num_misses[part_id][rw]++;

#ifdef BPOOL_ENABLE_LOCKS
	    __bpool_unlock(0, &pool->stats_lock );
#endif

	    /* copy key into node->id */
	    pool->ops.mwrite(pool->environment, BPOOL_KEY, node->id, id);
	    hashtab_insert(pool->table, (void *) node->id, (void *) node);
	    node->part_id = part_id;
	    node->valid = 1;
	    
	    /* update sizes */
	    pool->global_size++;
	    pool->part_sizes[part_id]++;

	    /* unlock big_lock so other reads proceed */
#ifdef BPOOL_ENABLE_LOCKS
	    assert(0 == __bpool_unlock(4, &pool->big_lock));
#endif

	    /* page fault to fill the block */
	    int ret=0; double fill_start_time, fill_stop_time;

	    fill_start_time = __bpool_time();
	    pool->ops.fill(pool->environment, node, &ret, tag);
	    fill_stop_time = __bpool_time();

	    /* stats: record fill */
#ifdef BPOOL_ENABLE_LOCKS
	    __bpool_lock(0, &pool->stats_lock );
#endif

	    pool->stats.global_num_fills[rw]++;
	    pool->stats.part_num_fills[part_id][rw]++;
	    pool->stats.global_lat_fills[rw] += (fill_stop_time - fill_start_time);
	    pool->stats.part_lat_fills[part_id][rw] += (fill_stop_time - fill_start_time);

#ifdef BPOOL_ENABLE_LOCKS
	    __bpool_unlock(0, &pool->stats_lock );
#endif
	    if(ret >= 0) {
	      if(!rw) {
		pool->ops.mread(pool->environment, BPOOL_BUF, node->buf, buf);
	      }
	      else { 
		pool->ops.mwrite(pool->environment, BPOOL_BUF, node->buf, buf);
		node->dirty = 1; 
	      }
	    } else {
	      G_ASSERT(0, "BPOOL: fill failed!!", HARD_ASSERT);
	      bpool_delete(pool,id, 0);
	      return ret;
	    }

#ifdef BPOOL_ENABLE_LOCKS
	    assert(0 == __bpool_unlock(5, &node->page_lock));
#endif
	    return CACHE_MISS;
	  }

	} /* end if(node) */

	/* no free node - ignore for now */
	G_ASSERT(0, "ERROR: bpool_get(): can't find any free nodes!\n", HARD_ASSERT);
	return -1;

      } /* end else - node == NULL */
    }
  }

  G_ASSERT(0, "ERROR: bpool_get(): some of the variables are NULL!\n", HARD_ASSERT);
  return -1;
}

/**
 * bpool_read
 * ----------
 * returns data from the buffer pool
 * calls fill() to fill empty block (if needed)
 * 
 * Returns bytes read. -1 on error
 * Only TRX thread use this function so do mining here
 **/

int bpool_read(struct bpool *pool, char *id, char *buf, void *tag) {

  int ret = 0;
  double start_time, stop_time;

  start_time = __bpool_time();
  ret =  __bpool_get(pool, id, buf, BPOOL_READ, tag);
  stop_time = __bpool_time();

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_lock(0, &pool->stats_lock );
#endif

  /* global numbers */
  pool->stats.global_lat_gets[BPOOL_STAT_READ] += (stop_time - start_time);
  if(ret == CACHE_HIT) {
    pool->stats.global_lat_hits[BPOOL_STAT_READ] += (stop_time - start_time);
  }
  if(ret == CACHE_MISS) {
    pool->stats.global_lat_misses[BPOOL_STAT_READ] += (stop_time - start_time);
  }

  /* partition numbers */
  if( pool->ops.part != NULL ) {
    unsigned part_id = pool->ops.part( pool->environment, tag, id );
    pool->stats.part_lat_gets[part_id][BPOOL_STAT_READ] += (stop_time - start_time);
    if(ret == CACHE_HIT) {
      pool->stats.part_lat_hits[part_id][BPOOL_STAT_READ] += (stop_time - start_time);
    }
    if(ret == CACHE_MISS) {
      pool->stats.part_lat_misses[part_id][BPOOL_STAT_READ] += (stop_time - start_time);
    }
  }

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_unlock(0, &pool->stats_lock );
#endif

  return ret;

}

/**
 * bpool_write
 * ----------
 * write to given block
 * calls fill() to fill empty block (if needed)
 * if(write_back) also updates disk, otherwise marks dirty
 *
 * Returns bytes read. -1 on error
 **/

int bpool_write(struct bpool *pool, char *id, char *buf, void *tag) {

  int ret =  0;
  double start_time, stop_time;

  start_time = __bpool_time();
  ret = __bpool_get(pool, id, buf, BPOOL_WRITE, tag);
  stop_time = __bpool_time();

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_lock(0, &pool->stats_lock );
#endif

  /* global stats */
  pool->stats.global_lat_gets[BPOOL_STAT_WRITE] += (stop_time - start_time);;
  if(ret == CACHE_HIT) {
    pool->stats.global_lat_hits[BPOOL_STAT_WRITE] += (stop_time - start_time);
  }
  if(ret == CACHE_MISS) {
    pool->stats.global_lat_misses[BPOOL_STAT_WRITE] += (stop_time - start_time);
  }
  /* partition stats */
  if( pool->ops.part != NULL ) {
    unsigned part_id = pool->ops.part( pool->environment, tag, id );
    pool->stats.part_lat_gets[part_id][BPOOL_STAT_WRITE] += (stop_time - start_time);
    if(ret == CACHE_HIT) {
      pool->stats.part_lat_hits[part_id][BPOOL_STAT_WRITE] += (stop_time - start_time);
    }
    if(ret == CACHE_MISS) {
      pool->stats.part_lat_misses[part_id][BPOOL_STAT_WRITE] += (stop_time - start_time);
    }
  }

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_unlock(0, &pool->stats_lock );
#endif

  return ret;

}

/**
 * bpool_delete
 * ----------
 * In some cases, I may have to force delete an item
 * use this for this case
 * TODO: if flush, then changes flushed otherwise discarded
 *
 * Returns
 **/

/* TODO: Looks fishy */
int bpool_delete(struct bpool *pool, char *id, int write_back) {
  if(pool && id) {
    struct bpool_node *node;

#ifdef BPOOL_ENABLE_LOCKS
    assert(0 == __bpool_lock(500, &pool->big_lock));
#endif

    node = hashtab_search(pool->table, id);
    if(node) {
#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == __bpool_lock(501, &node->page_lock));
#endif

      assert(!node->dirty);
      hashtab_delete(pool->table, id);
      /* TODO: add facility to reset blocks */
      /*memset(node->id, 0, pool->PAGE_ID_SIZE);
	memset(node->buf, 0, pool->PAGE_SIZE);*/
      node->refbit = 0;
      node->valid  = 0;
      node->dirty  = 0;

#ifdef BPOOL_ENABLE_LOCKS
      assert(0 == __bpool_unlock(501, &node->page_lock));
#endif

    }

#ifdef BPOOL_ENABLE_LOCKS
    assert(0 == __bpool_unlock(500, &pool->big_lock));
#endif

  } // if pool && id
  return -2;
} 
   

/*
 * bpool_flush_fast()
 * ------------------
 * Traverse the hashtab to delete the items
 * I hate holding the big_lock and breaking interfaces
 * But, if this makes flushing faster, then what the hey
 *
 */

static int bpool_flush_fast(struct bpool *pool, int should_discard) {

  if(pool) {
    unsigned long n_flushed = 0;

#ifdef BPOOL_ENABLE_LOCKS
    assert(0 == __bpool_lock(602, &pool->big_lock));
#endif

    struct hashtab_node *cur, *temp;
    struct hashtab *h = pool->table;
    unsigned long i = 0;
    assert(h);
    for (i = 0; i < h->size; i++) {
      cur = h->htable[i];
      while (cur != NULL) {
	struct bpool_node *node = (struct bpool_node *) cur->datum;
#ifdef BPOOL_ENABLE_LOCKS
	assert(0 == __bpool_lock(603, &node->page_lock));
#endif

	if(node->dirty) {
	  assert(node);
	  assert(node->dirty);
	  assert(node->valid);
	  int ret; 
	  pool->ops.flush(pool->environment, node, &ret, NULL);
	  node->dirty = 0;
	  if(should_discard) node->valid = 0;
	  n_flushed++;
	}

#ifdef BPOOL_ENABLE_LOCKS
	assert(0 == __bpool_unlock(603, &node->page_lock));
#endif

	temp = cur;
	cur = cur->next;
      }
    }    
#ifdef BPOOL_ENABLE_LOCKS
    assert(0 == __bpool_unlock(602, &pool->big_lock));
#endif

  }
  return -2;
}


int bpool_flush(struct bpool *pool, int should_discard) {
  return bpool_flush_fast(pool, should_discard);
}

/* Quotas and Partitions */
/* ----------------------------------------------------- */

static void __bpool_behave( struct bpool *pool ) {

  unsigned int i = 0;
  for(i=0; i < pool->num_partitions; i++) {
    unsigned too_much = 1; 
    
    /* is this partition over its quota? */
#ifdef BPOOL_ENABLE_LOCKS
    __bpool_lock( 100, &pool->big_lock );
#endif

    too_much = ( pool->part_sizes[i] > pool->part_quotas[i] );

#ifdef BPOOL_ENABLE_LOCKS
    __bpool_unlock( 100, &pool->big_lock );
#endif

    /* take away pages over quota */
    while( too_much ) {
      /* fprintf(stdout, "Info: Reducing partition[%u] to its quota...\n", i); */
      struct bpool_node *freed = __bpool_find_free_node(pool, BPOOL_READ, i, NULL);
      too_much = ( pool->part_sizes[i] > pool->part_quotas[i] );

#ifdef BPOOL_ENABLE_LOCKS      
      __bpool_unlock( 100, &pool->big_lock );
      __bpool_unlock( 100, &freed->page_lock );
#endif

    } /* end while */    
    
  } /* end for loop */

} /* end __bpool_behave() */

int bpool_setquota(struct bpool *pool, unsigned part_id, unsigned num_blocks ) {

  assert( pool != NULL );
  assert( part_id >= 0 && part_id < pool->num_partitions );
  assert( num_blocks > 0 && num_blocks <= pool->num_blocks );

  int erred = 0;
  unsigned i = 0;
  unsigned part_quotas[BPOOL_MAX_PARTITIONS];
  unsigned quota_sum = 0;


  /* Part 1: Error Checking */
#ifdef BPOOL_ENABLE_LOCKS
  __bpool_lock( 1, &pool->big_lock );  
#endif

  /* need to figure how this change affects the total size */
  for(i=0; i < pool->num_partitions; i++) {
    if(i != part_id ) { 
      quota_sum += pool->part_quotas[i];
      part_quotas[i] = pool->part_quotas[i];
    }
    else { 
      quota_sum += num_blocks; 
      part_quotas[i] = num_blocks;
    }
  }
  
  /* quotas should not exceed cache size */
  if( quota_sum > pool->num_blocks ) { erred = 1; }
  else { pool->part_quotas[part_id] = num_blocks; }

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_unlock( 1, &pool->big_lock );
#endif

  /* Part 2: Quota management */
  /* go and free blocks from partitions over
     their quota */
  if(!erred) {
    __bpool_behave( pool );
  }

  return (erred == 0) ? 0 : -1;

} /* end bpool_setquota() */


unsigned bpool_getquota(struct bpool *pool, unsigned part_id ) {

  unsigned quota = 0;
  assert( pool != NULL );
  assert( part_id >= 0 && part_id < pool->num_partitions );

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_lock( 1, &pool->big_lock );
#endif

  quota = pool->part_quotas[part_id];

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_unlock( 1, &pool->big_lock );
#endif 

  return quota;

} /* end bpool_getquota() */


/* Stats */
/* ----------------------------------------------------- */

int bpool_print_stats( struct bpool *pool, FILE *file) {

  int type = 0;

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_lock( 1, &pool->stats_lock );
#endif

  for(type = 0; type < BPOOL_STAT_MAX; type++ ) {
    fprintf(file, "NUM TYPE %2d GET %10d HIT %10d MISS %10d FILL %10d FLUSH %10d EVICT %10d VICTIM %10d\n",
	    type,
	    pool->stats.global_num_gets[type],
	    pool->stats.global_num_hits[type],
	    pool->stats.global_num_misses[type],
	    pool->stats.global_num_fills[type],
	    pool->stats.global_num_flushes[type],
	    pool->stats.global_num_evicts[type],
	    pool->stats.global_num_victims[type]
	    );
    fprintf(file, "LAT TYPE %2d GET %10.5lf HIT %10.5lf MISS %10.5lf FILL %10.5lf FLUSH %10.5lf EVICT %10.5lf VICTIM %10.5lf\n",
	    type,
	    pool->stats.global_lat_gets[type]/(pool->stats.global_num_gets[type] + 0.0),
	    pool->stats.global_lat_hits[type]/(pool->stats.global_num_hits[type] + 0.0),
	    pool->stats.global_lat_misses[type]/(pool->stats.global_num_misses[type] + 0.0),
	    pool->stats.global_lat_fills[type]/(pool->stats.global_num_fills[type] + 0.0),
	    pool->stats.global_lat_flushes[type]/(pool->stats.global_num_flushes[type] + 0.0),
	    pool->stats.global_lat_evicts[type]/(pool->stats.global_num_evicts[type] + 0.0),
	    pool->stats.global_lat_victims[type]/(pool->stats.global_num_victims[type] + 0.0)
	    );
  } /* end for type loop */

#ifdef BPOOL_ENABLE_LOCKS
  __bpool_unlock( 1, &pool->stats_lock );
#endif

  return 0;

} /* end bpool_stats() */
