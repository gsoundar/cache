/**
 * HASHTABLE
 * ---------
 * by Gokul Soundararajan
 *
 * A hashtable implementation to be used
 * by other pieces of code
 **/

#ifndef __HASHTAB_H__
#define __HASHTAB_H__


struct hashtab_node {
  void *key;
  void *datum;
  struct hashtab_node *next;
};

struct hashtab {
  unsigned int size;
  unsigned int nel;
  unsigned int  (*hash_value)(struct hashtab *h, void *key);
  int (*keycmp)(struct hashtab *h, void *key1, void *key2);
  struct hashtab_node **htable;
};

struct hashtab *hashtab_create(unsigned int (*hash_value)(struct hashtab *h, void *key),
                               int (*keycmp)(struct hashtab *h, void *key1, void *key2),
                               unsigned int size);
int             hashtab_insert(struct hashtab *h, void *k, void *d);
void*           hashtab_delete(struct hashtab *h, void *k);
void*           hashtab_search(struct hashtab *h, void *k);
void            hashtab_print(struct hashtab *h);
void            hashtab_destroy(struct hashtab *h, 
				void (*key_free)(void *ptr),
				void (*datum_free)(void *ptr)
				);
#endif /* __HASHTAB_H__ */
