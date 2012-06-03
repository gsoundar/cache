/**
 * HASHTABLE
 * ---------
 * by Gokul Soundararajan
 *
 * A hashtable to be used by all
 *
 **/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "hashtable.h"

struct hashtab *hashtab_create(unsigned int (*hash_value)(struct hashtab *h, void *key),
                               int (*keycmp)(struct hashtab *h, void *key1, void *key2),
                               unsigned int size) {

  /* malloc space */
  struct hashtab *table = NULL;
  table = calloc(1, sizeof(struct hashtab));
  assert( table && "ASSERT: Could not malloc space for h!!");
  table->htable = calloc(size, sizeof(struct hashtab_node *) );
  assert( table->htable && "ASSERT: Could not malloc space for htable!!");

  /* initialize it */
  table->hash_value = hash_value;
  table->keycmp = keycmp;
  table->nel = 0;
  table->size = size;

  int i = 0;
  for(i=0; i < table->size; i++) {
    table->htable[i] = NULL;
  }

  return table;

} /* end hashtab_create() */


int hashtab_insert(struct hashtab *h, void *k, void *d) {

  /* Check inputs */
  assert( h && "ASSERT: hashtable is NULL!!" );
  assert( k && "ASSERT: key is NULL!!" );
  assert( d && "ASSERT: data is NULL!!" );
  
  /* find slot id */
  unsigned slot_id = h->hash_value( h, k );
  assert(slot_id >=0 && slot_id < h->size && "ASSERT: slot_id is invalid!!" );

  /* entry must not exist already */
  if( hashtab_search( h, k) != NULL ) {
    fprintf(stderr, "Error: key already exists in hashtable!!\n");
    fflush(stderr);
    return -1;
  }

  /* create a new entry and add */
  struct hashtab_node *node = calloc( 1, sizeof(struct hashtab_node));
  assert(node && "ASSERT: Could not malloc space for new node!!");
  node->key = k; node->datum = d; node->next = NULL;

  /* Add it */
  node->next = h->htable[slot_id];
  h->htable[slot_id] = node;

  h->nel++;

  return 0;


} /* end hashtab_insert() */


void* hashtab_delete(struct hashtab *h, void *k) {


  /* Check inputs */
  assert( h && "ASSERT: hashtable is NULL!!" );
  assert( k && "ASSERT: key is NULL!!" );
  
  /* find slot id */
  unsigned slot_id = h->hash_value( h, k );
  assert(slot_id >=0 && slot_id < h->size && "ASSERT: slot_id is invalid!!" );

  /* entry must exist */
  if( hashtab_search( h, k) == NULL ) {
    fprintf(stderr, "Error: no such key available in hashtable!!\n");
    fflush(stderr);
    return NULL;
  }

  /* really delete it */
  assert( h->htable[slot_id] && "Error: slot has no entry!!" );
  if( h->keycmp(h, h->htable[slot_id]->key, k ) == 0 ) { /* root matched */
    struct hashtab_node *found_node = h->htable[slot_id];
    void *found_data = found_node->datum;
    h->htable[slot_id] = found_node->next;
    free(found_node);
    h->nel--;
    return found_data;
  } 
  /* go through the list */
  else {
    struct hashtab_node *cur_node=h->htable[slot_id]->next;
    struct hashtab_node *prev_node=h->htable[slot_id];
    void *found_data = NULL;
    while( cur_node != NULL ) {
      if( h->keycmp(h, cur_node->key, k) == 0 ) { /* found it */
	found_data = cur_node->datum;
	prev_node->next = cur_node->next;	
	free(cur_node);
	h->nel--;
	return found_data;
      }
      prev_node = cur_node;
      cur_node = cur_node->next;
    }
    return NULL;
  }

  return NULL;

} /* end hashtab_delete() */


void* hashtab_search(struct hashtab *h, void *k) {

  /* Check inputs */
  assert( h && "ASSERT: hashtable is NULL!!" );
  assert( k && "ASSERT: key is NULL!!" );

  /* find slot id */
  unsigned slot_id = h->hash_value( h, k );
  assert(slot_id >=0 && slot_id < h->size && "ASSERT: slot_id is invalid!!" );

  /* go through the list and find it */
  struct hashtab_node *p = h->htable[slot_id];
  while( p != NULL ) {
    if( h->keycmp(h, k, p->key) == 0 ) { /* matched */
      return p->datum;
    }
    p = p->next;
  }
  
  return NULL;

} /* end hashtab_search() */

void hashtab_print(struct hashtab *h) {

  unsigned i = 0;

  for(i=0; i < h->size; i++) {
    struct hashtab_node *node = h->htable[i];
    fprintf(stdout, "[%u]:", i);
    while(node != NULL) {
      fprintf(stdout, "[%u:%u] ", (unsigned) node, (unsigned) node->next);
      node=node->next;
    }
    fprintf(stdout, "\n");
  }

}/* end hashtab_print() */


void hashtab_destroy(struct hashtab *h, 
		     void (*key_free)(void *ptr),
		     void (*datum_free)(void *ptr)) {


  assert( 0 && "Fatal: hashtab_destroy() not implemented yet!!" );

} /* end hashtab_destroy() */
