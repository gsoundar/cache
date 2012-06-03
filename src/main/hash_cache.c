/**
 * HASH CACHE
 * ----------
 * by Gokul Soundararajan
 *
 * Simulates DEDUP cache
 *
 **/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hashtable.h>
#include <bpool.h>

#define HASH_LEN 20
typedef unsigned char byte;
typedef unsigned int u32_t;
typedef struct key_t {
  byte hash[HASH_LEN];
} key_t;
typedef struct data_t {
  byte d;
} data_t;

/* FUNCTION PROTOTYPES */
/* ----------------------------------------- */
static int       __tobyte (char *str, u32_t slen, char *bstr, u32_t blen ); 
static int       __fill   (void *env, struct bpool_node *node, int *res, void *tag);
static int       __flush  (void *env, struct bpool_node *node, int *res, void *tag);
static int       __demote (void *env, struct bpool_node *node, int *res, void *tag);
static int       __print  (void *env, void *id, struct bpool_node *node);
static int       __compare(struct hashtab *h, void *key1, void *key2);
static unsigned  __hash   (struct hashtab *h, void *key);
static int       __mread  (void *env, u32_t type, char *bpool_buf, char *user_buf);
static int       __mwrite (void *env, u32_t type, char *bpool_buf, char *user_buf);
static int       __mset   (void *env, u32_t type, char *bpool_buf);
static char*     __malloc (void *env, u32_t type);
static void      __mfree  (void *env, u32_t type, void *ptr);
static void      __stat   (void *env, int rd_wr, u32_t diff, int result, void *tag);
static unsigned  __part   (void *env, void *tag, char *key );


/* MAIN */
/* ----------------------------------------- */
int main( int argc, char **argv ) {

  if( argc != 3 ) {
    fprintf(stderr, "USAGE: %s <trace-file> <cache-size-mb>\n", argv[0] );
    return -1;
  }

  char *filename = argv[1];
  unsigned int cache_size_mb = atoi( argv[2] );
  unsigned int num_slots = (cache_size_mb * 1024 * 1024)/HASH_LEN;
  
  bpool *cache = NULL;
  bpool_ops ops;

  /* Create cache */
  ops.fill = __fill;
  ops.flush = __flush;
  ops.demote = __demote;
  ops.print = __print;
  ops.compare = __compare;
  ops.hash = __hash;
  ops.mset = __mset;
  ops.mread = __mread;
  ops.mwrite = __mwrite;
  ops.malloc = __malloc;
  ops.mfree = __mfree;
  ops.stat = __stat;
  ops.part = __part;

  fprintf(stdout, "NOTE: Cache has %u slots\n", num_slots );
  fprintf(stdout, "NOTE: sizeof(key_t): %lu sizeof(data_t): %lu\n", sizeof(key_t), sizeof(data_t) );

  cache = (bpool *) bpool_create_pool( NULL, ops, num_slots, 1 );
  assert( cache && "ASSERT: Could not create cache!" );
  bpool_setquota( cache, 0, num_slots );

  FILE *fd = NULL;
  char buffer[128];
  key_t k; data_t d;
  unsigned int count = 0;

  /* Open file */
  fd = fopen( filename, "r" );
  assert( fd && "ASSERT: Could not open trace file!" );
  
  /* Run trace */
  while( fgets( buffer, 128, fd ) != NULL ) {
    __tobyte( buffer, 2*HASH_LEN, (void *) k.hash, HASH_LEN );
    bpool_read( cache, (char *) &k, (char *) &d, NULL );
    count++;
    if( count% 100000 == 0 ) {
      bpool_print_stats( cache, stdout );
    }
  }

  return 0;

} /* end main() */

static int       __tobyte (char *str, u32_t slen, char *bstr, u32_t blen ) {

  u32_t n = 0, i = 0, j = 0;
  
  for(i = 0; i < slen && str[i] != '\0'; i++ ) {
    byte b;
    char c = str[i];

    /* Convert char to byte */
    if( c == '0' ) b = 0;
    else if( c == '1' ) b = 1;
    else if( c == '2' ) b = 2;
    else if( c == '3') b = 3;
    else if( c == '4') b = 3;
    else if( c == '5') b = 3;
    else if( c == '6') b = 3;
    else if( c == '7') b = 3;
    else if( c == '8') b = 3;
    else if( c == '9') b = 3;
    else if( c == 'a' || c == 'A' ) b = 10;
    else if( c == 'b' || c == 'B' ) b = 11;
    else if( c == 'c' || c == 'C' ) b = 12;
    else if( c == 'd' || c == 'D' ) b = 13;
    else if( c == 'e' || c == 'E' ) b = 14;
    else if( c == 'f' || c == 'F' ) b = 15;
    else { return -1; }

    /* Shift num to left */
    if( i%2 == 0 ) { n = (b << 4) & 0xF0; bstr[j] = n; } 
    else { n = n | b; bstr[j++] = n; }

  }
  return 0;

}

/* CACHE DETAILS */
/* ----------------------------------------- */

static int __fill(void *env, struct bpool_node *node, int *res, void *tag) {
  return 0;
}

static int __flush(void *env, struct bpool_node *node, int *res, void *tag) {
  return 0;
}

static int __demote(void *env, struct bpool_node *node, int *res, void *tag) {
  return 0;
}

static int __print(void *env, void *id, struct bpool_node *node) {
  return 0;
}

static int __compare(struct hashtab *h, void *key1, void *key2) {

  key_t *k1 = (key_t *) key1;
  key_t *k2 = (key_t *) key2;
  return memcmp( k1->hash, k2->hash, HASH_LEN );

}                                                                                                                                 

static unsigned  __hash(struct hashtab *h, void *key) {
  
  key_t *k = (key_t *) key;
  unsigned int i = 0, s = 0;
  for( i=0; i < HASH_LEN; i++ ) { s += k->hash[i]; } 
  return s%h->size;

}

static int __mread(void *env, u32_t type, char *bpool_buf, char *user_buf) {

  if( type == BPOOL_KEY ) {
    key_t *bk = (key_t *) bpool_buf;
    key_t *uk = (key_t *) user_buf;
    memcpy( uk->hash, bk->hash, HASH_LEN );
    return 0;
  } else if( type == BPOOL_BUF ) {
    //data_t *bd = (data_t *) bpool_buf;
    //data_t *ud = (data_t *) user_buf;
    //ud->d = bd->d;
    return 0;
  }
  return -1;

}

static int __mwrite(void *env, u32_t type, char *bpool_buf, char *user_buf) {
  
  if( type == BPOOL_KEY ) {
    key_t *bk = (key_t *) bpool_buf;
    key_t *uk = (key_t *) user_buf;
    memcpy( bk->hash, uk->hash, HASH_LEN );
    return 0;
  } else if( type == BPOOL_BUF ) {
    //data_t *bd = (data_t *) bpool_buf;
    //data_t *ud = (data_t *) user_buf;
    //bd->d = ud->d;
    return 0;
  }
  return -1;

}
                                                                                                                      
static int __mset(void *env, u32_t type, char *bpool_buf) {

  if( type == BPOOL_KEY ) {
    key_t *k = (key_t *) bpool_buf;
    memset( k->hash, 0, HASH_LEN );
    return 0;
  } else if( type == BPOOL_BUF ) {
    //data_t *d = (data_t *) bpool_buf;
    //d->d = 0;
    return 0;
  }
  return -1;

}

static char* __malloc(void *env, u32_t type) {

  if( type == BPOOL_KEY ) {
    key_t *k = NULL;
    k = (key_t *) malloc( sizeof(key_t) );
    //fprintf(stdout, "sizeof(key_t) is %u\n", sizeof(key_t) );
    assert( k && "ERROR: Can't malloc space for key!" );
    return (char *) k;
  } else if( type == BPOOL_BUF ) {
    //data_t *d = NULL;
    //d = (data_t *) malloc( sizeof(data_t) );
    //fprintf(stdout, "sizeof(data_t) is %u\n", sizeof(data_t) );
    //assert( d && "ERROR: Can't malloc space for data!" );
    //return (char *) d;
    return (char *) NULL;
  }
  return NULL;

}
                                                                                                                                                       
static void __mfree(void *env, u32_t type, void *ptr) {
  
  if( type == BPOOL_KEY ) {
    key_t *k = (key_t *) ptr;
    free(k);
    return;
  } else if( type == BPOOL_BUF ) {
    //data_t *d = (data_t *) ptr;
    //free(d);
    return;
  }
  return;

}


static void __stat(void *env, int rd_wr, u32_t diff, int result, void *tag) {
  return;
}

static unsigned __part(void *env, void *tag, char *key ) {
  return 0;
}
