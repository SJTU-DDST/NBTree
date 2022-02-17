#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#ifdef USE_PMDK
#include <libpmemobj.h>
#endif
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#include <tbb/spin_rw_mutex.h>
#include "util.h"
#include "timer.h"
#define eADR
#define NVM
#define CACHE_LINE 64
#define PAGESIZE 512
#define LEAF_NODE_SIZE 31
#define IS_FORWARD(c) (c % 2 == 0)
#define FULL ((1llu << LEAF_NODE_SIZE) - 1)
#define SYNC_MASK 1llu << 63
#define COPY_MASK 1llu << 62
#define MASK (SYNC_MASK | COPY_MASK)

using entry_key_t = uint64_t;

pthread_mutex_t print_mtx;

const uint64_t SPACE_PER_THREAD = 512ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 1ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_addr;
extern __thread char *curr_addr;

const uint64_t MEM_PER_THREAD = 1ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t MEM_OF_MAIN_THREAD = 1ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_mem;
extern __thread char *curr_mem;

typedef tbb::speculative_spin_rw_mutex speculative_lock_t;
typedef speculative_lock_t::scoped_lock htm_lock;
using namespace std;

void *data_alloc(size_t size)
{
  void *ret = curr_addr;
  curr_addr += size;

  return ret;
}

void *leaf_alloc(size_t size)
{
  void *ret = curr_mem;
  memset(ret, 0, size);
  curr_mem += size;

  return ret;
}

class
    page;
class leaf_node_t;
class data_node_t;
class inner_node_t;

class btree
{
private:
  int height;
  char *root;

public:
  data_node_t *data_anchor = NULL;
  leaf_node_t *anchor = NULL;
  speculative_lock_t mtx;
  int c;
  btree();
  ~btree();
  void setNewRoot(char *new_root, leaf_node_t *leaf = NULL);
  void btree_insert_internal(char *, entry_key_t, char *, uint32_t, leaf_node_t *leaf = NULL);
  char *btree_search(entry_key_t);
  void print();
  void check();
  bool insert(entry_key_t, char *); 
  bool remove(entry_key_t);
  bool update(entry_key_t, char *);
  char *search(entry_key_t);
private:
  static const uint64_t kFNVPrime64 = 1099511628211;
  unsigned char hashfunc(uint64_t val);
  int find_item(entry_key_t key, leaf_node_t *leaf, uint8_t hash);
  bool modify(leaf_node_t *leaf, int pos, entry_key_t key, char *right);
  leaf_node_t *find_leaf(entry_key_t key, inner_node_t **parent, bool debug, bool print);
  leaf_node_t *find_pred_leaf(entry_key_t key, char **prev, inner_node_t **parent);
  leaf_node_t *inner_node_search(entry_key_t key, char **prev, inner_node_t **parent);
  leaf_node_t *inner_node_search(entry_key_t key, inner_node_t **parent, bool debug, bool print);
  leaf_node_t *SplitLeaf(leaf_node_t *leaf, inner_node_t *parent, leaf_node_t *, entry_key_t, bool, int);
  // help function for split
  void copy(leaf_node_t *leaf);
  void sync(leaf_node_t *leaf);
  void update_prev_node(leaf_node_t *leaf, inner_node_t *parent, leaf_node_t *prev = NULL);
  void update_parent(leaf_node_t *leaf, inner_node_t *parent);
  bool check_pred(entry_key_t key, char **prev, inner_node_t *parent, int id);
  bool check_parent(char *left, entry_key_t key, char *right, uint32_t level, inner_node_t *parent, leaf_node_t *leaf, int id);

  friend class page;
  friend class inner_node_t;
  friend class leaf_node_t;
};

class page
{
  friend class btree;
};

class header
{
private:
  page *leftmost_ptr;        // 8 bytes
  inner_node_t *sibling_ptr; // 8 bytes
  inner_node_t *pred_ptr;    // 8 bytes
  entry_key_t high_key;      // 8 bytes
  entry_key_t low_key;       // 8 bytes
  // uint32_t level;            // 4 bytes
  uint16_t level;         // 2 byte
  uint8_t spin_lock;      // 1 byte
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  friend class page;
  friend class btree;
  friend class inner_node_t;

public:
  header()
  {
    leftmost_ptr = NULL;
    sibling_ptr = NULL;
    pred_ptr = NULL;
    high_key = ~(0llu);
    low_key = 0;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }
};

class entry
{
private:
  entry_key_t key; // 8 bytes
  char *ptr;       // 8 bytes

public:
  entry()
  {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class page;
  friend class btree;
  friend class inner_node_t;
  friend class leaf_node_t;
  friend class data_node_t;
};

const int cardinality = (PAGESIZE - sizeof(header)) / sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class data_node_t : public page
{
public:
  entry kv[LEAF_NODE_SIZE]; // 16*leaf_node_size byte
  data_node_t *next;        // 8 byte
  data_node_t *log;         // 8 byte

  data_node_t()
  {
    next = NULL;
    log = NULL;
  }

  int check_node(entry_key_t low = 0, entry_key_t high = ~(0llu))
  {
    int count = 0;
    for (int i = 0; i < LEAF_NODE_SIZE; i++)
    {
      if (kv[i].key != 0)
      {
        if (kv[i].key < low || kv[i].key >= high)
        {
          printf("low:%lu, high:%lu\n", low, high);
          print_node();
        }
        count++;
        assert(kv[i].key >= low);
        assert(kv[i].key < high);
      }
    }
    return count;
  }

  void print_node()
  {
    for (int i = 0; i < LEAF_NODE_SIZE; i++)
    {
      uint64_t value = (uint64_t)(kv[i].ptr);
      value = (value & (~MASK));
      printf("%d\t%lu\t%lu\n", i, kv[i].key, value);
    }
  }

  inline void _prefetch()
  {
    char *start_ptr = (char *)this;
    int length = PAGESIZE / 64;
    while (length-- > 0)
    {
      prefetch(start_ptr);
      start_ptr += 64;
    }
  }
};

class leaf_node_t : public page
{
public:
  alignas(64) uint8_t finger_prints[LEAF_NODE_SIZE];
  uint32_t bitmap;
  uint32_t number;
  entry_key_t high_key;
  entry_key_t low_key;
  data_node_t *data;
  leaf_node_t *next;
  leaf_node_t *log;
  std::mutex *mtx;
  bool copy_flag;
  bool sync_flag;
  bool prev_flag;
  bool fin_flag;

  leaf_node_t()
  {
    init();
  }
  void init()
  {
    mtx = new std::mutex();
    number = 0;
    bitmap = 0;
    data = (data_node_t *)data_alloc(sizeof(data_node_t));
    log = NULL;
    next = NULL;
    copy_flag = sync_flag = prev_flag = fin_flag = 0;
  }
  void lock()
  {
    mtx->lock();
  }
  void unlock()
  {
    mtx->unlock();
  }
  uint8_t get_number()
  {
    return number;
  }

  bool set_slot(int pos)
  {
    uint32_t prev, curr;
    do
    {
      prev = bitmap;
      if (prev & (1 << 31))
        return false;
      curr = (prev | (1llu << pos));
    } while (!__sync_bool_compare_and_swap(&bitmap, prev, curr));

    return true;
  }

  void set_split_bit()
  {
    uint32_t prev, curr;
    do
    {
      prev = bitmap;
      if (prev & (1 << 31))
        return;
      curr = (prev | (1llu << 31));
    } while (!__sync_bool_compare_and_swap(&bitmap, prev, curr));
  }

  bool check_slot(int i)
  {
    return ((bitmap & (1 << i)) != 0);
  }

  bool check_split()
  {
    return ((bitmap & (1 << 31)) != 0);
  }

  void print_node()
  {
    printf("leaf address:%p\n", this);
    printf("new leaf address:%p\n", log);
    printf("split_lock:%d\n", check_split());
    printf("finish split:%d\n", fin_flag);
    printf("finish prev:%d\n", prev_flag);
    printf("low_key:%lu\n", low_key);
    printf("high_key:%lu\n", high_key);
    printf("number:%d\n", number);
    printf("bitmap:%x\n", bitmap);
    printf("next:%p\n", (leaf_node_t *)next);
  }
  void check_node(entry_key_t low)
  {

    if (data != NULL)
    {
      data->check_node(low, high_key);
    }
    else
      printf("can't find the data!\n");
  }
};

class inner_node_t : public page
{
private:
  header hdr;                 // header in persistent memory, 16 bytes
  entry records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree;

  inner_node_t(uint32_t level = 0)
  {
    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  inner_node_t(page *left, entry_key_t key, page *right, uint32_t level = 0)
  {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;
  }

  void *operator new(size_t size)
  {
    void *ret;
    int x;
    x = posix_memalign(&ret, 64, size);
    return ret;
  }

  inline int count()
  {
    uint8_t previous_switch_counter;
    int count = 0;
    do
    {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL)
      {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0)
      {
        count = 0;
        while (records[count].ptr != NULL)
        {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(entry_key_t key)
  {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i)
    {
      if (!shift && records[i].key == key)
      {
        records[i].ptr = (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift)
      {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;
      }
    }

    if (shift)
    {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true)
  {
    bool ret = remove_key(key);

    return ret;
  }

  // revised
  // Insert the key in the inner node
  inline void insert_key(char *left, entry_key_t key, char *ptr, int *num_entries, bool flush = true,
                         bool update_last_index = true)
  {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0)
    { // this page is empty
      entry *new_entry = (entry *)&records[0];
      entry *array_end = (entry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;

      array_end->ptr = (char *)NULL;

      // if (hdr.pred_ptr != NULL)
      //   *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
    }
    else
    {
      int i = *num_entries - 1, inserted = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      // FAST
      for (i = *num_entries - 1; i >= 0; i--)
      {
        if (key < records[i].key)
        {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;
        }
        else
        {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;

          if (left != NULL)
          {
            records[i].ptr = left;
          }
          inserted = 1;
          break;
        }
      }
      if (inserted == 0)
      {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
        if (left != NULL)
        {
          hdr.leftmost_ptr = (page *)left;
        }
      }
    }

    if (update_last_index)
    {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // revised
  // Insert a new key in the inner node- FAST and FAIR
  page *store(btree *bt, char *left, entry_key_t key, char *right,
              bool flush, bool with_lock, page *invalid_sibling = NULL, leaf_node_t *leaf = NULL)
  {
    // 1. HTM begin
    htm_lock l;
    if (with_lock)
    {
      l.acquire(bt->mtx);
      if (leaf)
      {
        if (leaf->fin_flag)
        {
          l.release();
          return NULL;
        }
      }
    }
    if (hdr.is_deleted)
    {
      if (with_lock)
      {
        l.release();
      }
      return NULL;
    }

    // 2. Check if the leaf is correct
    if (key >= hdr.high_key)
    {
      if (with_lock)
        l.release();
      if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling))
      {
        // should pass the leaf parameter to sibling pointer
        return hdr.sibling_ptr->store(bt, left, key, right,
                                      true, with_lock, invalid_sibling, leaf);
      }
      else
        return NULL;
    }
    else if (key < hdr.low_key)
    {
      if (with_lock)
        l.release();
      if (hdr.pred_ptr && (hdr.pred_ptr != invalid_sibling))
      {
        // should pass the leaf parameter to sibling pointer
        return hdr.pred_ptr->store(bt, left, key, right,
                                   true, with_lock, invalid_sibling, leaf);
      }
      else
        return NULL;
    }

    // 3. Insert
    register int num_entries = count();
    if (num_entries < cardinality - 1)
    {
      // FAST
      insert_key(left, key, right, &num_entries);
      if (with_lock)
      {
        if (leaf)
        {
          assert(!leaf->fin_flag);
          leaf->fin_flag = 1;
        }
        l.release();
      }
      return this;
    }
    else
    {
      // FAIR
      inner_node_t *sibling = new inner_node_t(hdr.level);
      register int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      int sibling_cnt = 0;
      for (int i = m + 1; i < num_entries; ++i)
      {
        sibling->insert_key(NULL, records[i].key, records[i].ptr, &sibling_cnt, false);
      }
      sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
      sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      sibling->hdr.pred_ptr = this;
      if (sibling->hdr.sibling_ptr != NULL)
        sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
      sibling->hdr.high_key = hdr.high_key;
      sibling->hdr.low_key = split_key;
      hdr.sibling_ptr = sibling;
      hdr.high_key = split_key;

      records[m].ptr = NULL;
      hdr.last_index = m - 1;
      num_entries = hdr.last_index + 1;
      page *ret;

      // insert the key
      if (key < split_key)
      {
        insert_key(left, key, right, &num_entries);
        ret = this;
      }
      else
      {
        if (key < sibling->hdr.low_key)
        {
          print();
          sibling->print();
          assert(false);
        }
        sibling->insert_key(left, key, right, &sibling_cnt);
        ret = sibling;
      }
      // Set a new root or insert the split key to the parent
      if (bt->root == (char *)this)
      {
        // only one node can update the root ptr
        printf("Root height:%d!\n", hdr.level + 2);
        inner_node_t *new_root = new inner_node_t((page *)this, split_key, sibling,
                                                  hdr.level + 1);
        bt->setNewRoot((char *)new_root);
        if (with_lock)
        {
          if (leaf)
          {
            assert(!leaf->fin_flag);
            leaf->fin_flag = 1;
          }
          l.release();
        }
      }
      else
      {
        if (with_lock)
        {
          if (leaf)
          {
            assert(!leaf->fin_flag);
            leaf->fin_flag = 1;
          }
          l.release();
        }
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }

      return ret;
    }
  }

  page *linear_search(entry_key_t key, bool debug = false)
  {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    do
    {

      previous_switch_counter = hdr.switch_counter;
      ret = NULL;
      if (IS_FORWARD(previous_switch_counter))
      {
        if (key < hdr.low_key)
        {
          if ((t = (char *)(hdr.pred_ptr)) != NULL)
          {
            return ((inner_node_t *)t)->linear_search(key, debug);
          }
          else
            return NULL;
        }
        // modified by zbw: first read the left pointer, then compare the key
      Again1:
        if (key < (k = records[0].key))
        {
          t = (char *)hdr.leftmost_ptr;
          if (k == records[0].key)
          {
            ret = t;
            if (debug)
              printf("%lu is in pos = leftmost of %p, that is %p\n", key, this, ret);
            continue;
          }
          else
          {
            goto Again1;
          }
        }

        for (i = 1; records[i].ptr != NULL; ++i)
        {
          if (key < (k = records[i].key))
          {
            t = records[i - 1].ptr;
            if (k == records[i].key)
            {
              ret = t;
              if (debug)
                printf("%lu is in pos = %d of %p, that is %p\n", key, i - 1, this, ret);
              break;
            }
            else
            {
              i--;
            }
          }
          else if (debug)
            printf("skip %lu \n", k);
        }

        if (!ret)
        {
          ret = records[i - 1].ptr;
          if (debug)
            printf("%lu is in pos = rightmost of %p, that is %p\n", key, this, ret);
          continue;
        }
      }
      else
      { // search from right to left
        for (i = count() - 1; i >= 0; --i)
        {
          if (key >= (k = records[i].key))
          {
            if (i == 0)
            {
              if ((char *)hdr.leftmost_ptr != (t = records[i].ptr))
              {
                ret = t;
                break;
              }
            }
            else
            {
              if (records[i - 1].ptr != (t = records[i].ptr))
              {
                ret = t;
                break;
              }
            }
          }
        }
      }
      if (hdr.switch_counter != previous_switch_counter)
      {
        printf("search %lu retry!\n", key);
      }
      // assert(hdr.switch_counter == previous_switch_counter);
    } while (hdr.switch_counter != previous_switch_counter);

    if ((t = (char *)hdr.sibling_ptr) != NULL)
    {
      if (key >= hdr.high_key)
      {
        return ((inner_node_t *)t)->linear_search(key, debug);
      }
    }

    if (ret)
    {
      return (page *)ret;
    }
    else
    {
      assert(ret);
      return (page *)hdr.leftmost_ptr;
    }

    return NULL;
  }
  // linear search the leaf and its previous leaf in inner node
  char *linear_search_pred(entry_key_t key, char **pred, inner_node_t **parent, bool debug = false)
  {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t, *t1;
    entry_key_t k, k1;
    int pred_pos;
    int skip;
    entry_key_t skip_key, pred_key;
    char *skip_left, *skip_right, *pred_left, *pred_right;

    do
    {
      previous_switch_counter = hdr.switch_counter;
      ret = NULL;
      if (debug)
        printf("begin find prev!\n");
      if (IS_FORWARD(previous_switch_counter))
      {
        if (key < hdr.low_key)
        {
          if ((t = (char *)(hdr.pred_ptr)) != NULL)
          {
            parent = (inner_node_t **)&t;
            return ((inner_node_t *)t)->linear_search_pred(key, pred, parent, debug);
          }
          else
            return NULL;
        }

      Again2:
        *pred = NULL;
        if (key < (k = records[0].key))
        {
          t = (char *)hdr.leftmost_ptr;
          if (k == records[0].key)
          {
            if (hdr.pred_ptr != NULL)
            {
              *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
              pred_pos = -1;
              if (debug)
                printf("line 798, *pred=%p\n", *pred);
            }
            ret = t;
            continue;
          }
          else
          {
            goto Again2;
          }
        }
        else
        {
          *pred = (char *)(hdr.leftmost_ptr);
          if (debug)
            printf("line 808, *pred=%p\n", *pred);
        }

        for (i = 1; records[i].ptr != NULL; ++i)
        {
          if (key < (k = records[i].key))
          {
            t = records[i - 1].ptr;
            if (k == records[i].key)
            {
              ret = t;
              break;
            }
            else
            {
              i--;
            }
          }
          else
          {
            *pred = records[i - 1].ptr;
            if (debug)
              printf("line 824, *pred=%p\n", *pred);
          }
        }

        if (!ret)
        {
          ret = records[i - 1].ptr;
          continue;
        }
      }
      else
      { // search from right to left
        bool once = true;
        for (i = count() - 1; i >= 0; --i)
        {
          if (key >= (k = records[i].key))
          {
            // find the correct position
            if (i == 0)
            {
              if ((char *)hdr.leftmost_ptr != (t = records[i].ptr))
              {
                ret = t;
                if (hdr.pred_ptr != NULL)
                  *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                break;
              }
            }
            else
            {
              if ((*pred = records[i - 1].ptr) != (t = records[i].ptr))
              {
                ret = t;
                break;
              }
            }
          }
        }
      }
    } while (hdr.switch_counter != previous_switch_counter);

    if ((t = (char *)hdr.sibling_ptr) != NULL)
    {
      if (key >= hdr.high_key)
      {
        *parent = (inner_node_t *)t;
        // printf("Line 1094:search %llu in the sibling ptr %p!\n", key, t);
        return ((inner_node_t *)t)->linear_search_pred(key, pred, parent, debug);
      }
    }
    assert(ret);
    if (ret)
    {
      return ret;
    }
    else
      return (char *)hdr.leftmost_ptr;

    return NULL;
  }
  // print a node
  void print()
  {

    printf("[print node][%d] internal %p \n", this->hdr.level, this);
    printf("high key:%lu\n", hdr.high_key);
    printf("low key:%lu\n", hdr.low_key);
    if (hdr.leftmost_ptr != NULL)
    {
      printf("leftmost_ptr:[%p] ", hdr.leftmost_ptr);
    }

    for (int i = 0; records[i].ptr != NULL; ++i)
    {
      printf("key[%d]:[%ld], ", i, records[i].key);
      printf("ptr[%d]:[%p] ", i, records[i].ptr);
    }

    printf("\n[%p] ", hdr.sibling_ptr);

    printf("\n");
  }
};

/*
 * class btree
 */
btree::btree()
{
  c++;
  anchor = new leaf_node_t;
  anchor->high_key = (~0llu);
  anchor->low_key = 0;
  root = (char *)anchor;
  data_anchor = anchor->data;
  height = 1;
  printf("***** New NBTree **** \n");
}

btree::~btree()
{
}

void btree::setNewRoot(char *new_root, leaf_node_t *leaf)
{
  if (leaf == NULL)
  {
    this->root = (char *)new_root;
    ++height;
  }
  else
  {
    htm_lock l;
    l.acquire(mtx);
    if (!leaf->fin_flag)
    {
      // printf("old root:%p!\n", this->root);
      this->root = (char *)new_root;
      ++height;
      leaf->fin_flag = 1;
      l.release();
      printf("New root:%p!\n", new_root);
    }
    else
    {
      printf("failed!\n");
      l.release();
    }
  }
}

void btree::print()
{
  int i = 0;
  leaf_node_t *leaf = anchor;
  while (leaf != NULL)
  {
    leaf->print_node();
    leaf = (leaf_node_t *)(leaf->next);
  }
  printf("\n");
}

void btree::check()
{
  int i = 0;
  leaf_node_t *leaf = anchor;
  entry_key_t high = 0;
  while (leaf != NULL)
  {
    leaf->check_node(high);
    high = leaf->high_key;
    leaf = (leaf_node_t *)(leaf->next);
  }
  printf("correct!\n");
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right, uint32_t level, leaf_node_t *leaf)
{
  if (level > ((inner_node_t *)root)->hdr.level)
    return;

  inner_node_t *p = (inner_node_t *)(this->root);

  while (p->hdr.level > level)
    p = (inner_node_t *)p->linear_search(key);

  p->store(this, left, key, right, true, true, NULL, leaf);
}

// find the leaf
leaf_node_t *btree::find_leaf(entry_key_t key, inner_node_t **parent = NULL, bool debug = false, bool print = false)
{
  page *p = (page *)root;
  inner_node_t *inner;
  if (parent != NULL)
    *parent = NULL;
  if (height > 1)
  {
    // search down to the leaf node
    inner = (inner_node_t *)p;
    // assert(inner!=NULL);
    while (inner->hdr.level > 1)
    {
      if (debug)
      {
        printf("search %lu in level %d node %p\n", key, inner->hdr.level, inner);
        if (print)
        {
          printf("-------------\n");
          inner->print();
        }
      }
      p = inner->linear_search(key, debug);
      inner = (inner_node_t *)p;
    }
    if (debug)
    {
      printf("search %lu in level %d node %p\n", key, inner->hdr.level, inner);
      if (print)
        inner->print();
    }
    if (parent != NULL)
      *parent = inner;
    p = inner->linear_search(key, debug);
  }
  return (leaf_node_t *)p;
}

// find the leaf and its previous leaf
leaf_node_t *btree::find_pred_leaf(entry_key_t key, char **prev, inner_node_t **parent)
{
  inner_node_t *pln;
  leaf_node_t *leaf;
  if (height == 1)
  {
    *parent = NULL;
    *prev = NULL;
    // printf("height is 1!\n");
    return (leaf_node_t *)root;
  }

  pln = (inner_node_t *)root;
  while (pln->hdr.level > 1)
    pln = (inner_node_t *)(pln->linear_search(key));
  *parent = pln;
  // find the previous leaf
  leaf = (leaf_node_t *)(pln->linear_search_pred(key, prev, parent, false));

  return leaf;
}

leaf_node_t *btree::inner_node_search(entry_key_t key, inner_node_t **parent = NULL, bool debug = false, bool print = false)
{
  leaf_node_t *leaf;
  bool retry;
  do
  {
    retry = false;
    leaf = find_leaf(key);
    // assert(key <= leaf->high_key);
    while (key >= leaf->high_key)
    {
      leaf = (leaf_node_t *)(leaf->next);
      if (leaf == NULL)
        return NULL;
      // assert(key <= inserted_leaf->high_key);
    }
    if (key < leaf->low_key)
    {
      retry = true;
    }
  } while (retry);

  return leaf;
}

leaf_node_t *btree::inner_node_search(entry_key_t key, char **prev, inner_node_t **parent)
{
  leaf_node_t *leaf;
  while (true)
  {
    leaf = find_pred_leaf(key, prev, parent);
    while (key >= leaf->high_key)
    {
      leaf = (leaf_node_t *)(leaf->next);
      if (leaf == NULL)
        return NULL;
    }
    if (key < leaf->low_key)
    {
      continue;
    }
    break;
  }

  return leaf;
}

int btree::find_item(entry_key_t key, leaf_node_t *leaf, uint8_t hash)
{
  for (int i = 0; i < leaf->number; ++i)
  {
    if (leaf->finger_prints[i] == hash)
    {
      if (leaf->data->kv[i].key == key)
      {
        return i;
      }
    }
  }
  return -1;
}

unsigned char btree::hashfunc(uint64_t val)
{
  unsigned char hash = 123;
  int i;
  for (i = 0; i < sizeof(uint64_t); i++)
  {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hash = hash ^ octet;
    hash = hash * kFNVPrime64;
  }
  return hash;
}

leaf_node_t *btree::SplitLeaf(leaf_node_t *leaf, inner_node_t *parent, leaf_node_t *prev = NULL, entry_key_t key = 0, bool debug = false, int id = 0)
{
  entry_key_t split_key;
  leaf_node_t *firleaf, *secleaf;

  // 1. copy the entry from old leaf to new leaf
  copy(leaf);

  // 2. sync the update/delete happened in copy phase
  sync(leaf);

  // 3. update the next pointer of previous node
  update_prev_node(leaf, parent, prev);

  // 4. update the parent
  update_parent(leaf, parent);

  leaf_node_t *inserted_leaf = leaf->log;
  if (key != 0)
  {
    if (key >= inserted_leaf->high_key)
    {
      inserted_leaf = (leaf_node_t *)(inserted_leaf->next);
    }
  }
  return inserted_leaf;
}

bool btree::check_pred(entry_key_t key, char **prev, inner_node_t *parent, int id = 0)
{
  leaf_node_t *leaf;

  if (parent != NULL && key < parent->hdr.high_key && key >= parent->hdr.low_key)
  {
    // find the previous leaf
    leaf = (leaf_node_t *)(parent->linear_search_pred(key, prev, (inner_node_t **)&parent, false));
    return true;
  }
  else
    return false;
}

bool btree::check_parent(char *left, entry_key_t key, char *right, uint32_t level, inner_node_t *parent, leaf_node_t *leaf, int id = 0)
{
  if (level > ((inner_node_t *)root)->hdr.level)
    return false;

  if (parent != NULL && key < parent->hdr.high_key && key >= parent->hdr.low_key)
  {
    parent->store(this, left, key, right, true, true, NULL, leaf);
    return true;
  }
  else
    return false;
}

void btree::copy(leaf_node_t *leaf)
{
  if (leaf->log != NULL)
    return;

  // 1. find split key
  int count = 0;
  entry_key_t keys[LEAF_NODE_SIZE];
  entry_key_t splitKey;
  for (int i = 0; i < LEAF_NODE_SIZE; i++)
  {
    keys[count] = leaf->data->kv[i].key;
    if (keys[count] != 0 && leaf->check_slot(i))
      count++;
  }
  std::sort(keys, keys + count);
  splitKey = keys[count / 2];

  // 2. alllocate leaf and data
  leaf_node_t *firleaf = (leaf_node_t *)leaf_alloc(sizeof(leaf_node_t));
  leaf_node_t *secleaf = (leaf_node_t *)leaf_alloc(sizeof(leaf_node_t));
  firleaf->init();
  secleaf->init();
  // data_node_t *firdata = (data_node_t *)data_alloc(sizeof(data_node_t));
  // data_node_t *secdata = (data_node_t *)data_alloc(sizeof(data_node_t));
  // firleaf->data = firdata;
  // secleaf->data = secdata;

  // 3. copy the entry to the new leaf
  entry_key_t key;
  uint64_t value;
  leaf_node_t *node[2];
  int len[2];
  int c;
  node[0] = firleaf;
  node[1] = secleaf;
  len[0] = len[1] = 0;
  for (int i = 0; i < LEAF_NODE_SIZE; i++)
  {
    key = leaf->data->kv[i].key;
    if (key != 0 && leaf->check_slot(i))
    {
      if (key >= splitKey)
        c = 1;
      else
        c = 0;
      value = uint64_t(leaf->data->kv[i].ptr);
      value |= MASK;
      node[c]->finger_prints[len[c]] = leaf->finger_prints[i];
      node[c]->data->kv[len[c]].key = leaf->data->kv[i].key;
      node[c]->data->kv[len[c]].ptr = (char *)value;
      len[c]++;
    }
  }

  // 4. set the infomation
  firleaf->number = len[0];
  firleaf->bitmap = (1llu << len[0]) - 1;
  secleaf->number = len[1];
  secleaf->bitmap = (1llu << len[1]) - 1;
  firleaf->high_key = splitKey;
  secleaf->high_key = leaf->high_key;
  firleaf->low_key = leaf->low_key;
  secleaf->low_key = splitKey;
  firleaf->next = secleaf;
  secleaf->next = leaf->next;
  firleaf->data->next = secleaf->data;
  secleaf->data->next = leaf->data->next;

  // 5. commit copy
  __sync_bool_compare_and_swap(&(leaf->log), NULL, firleaf);
  leaf->data->log = leaf->log->data;
}

void btree::sync(leaf_node_t *leaf)
{
  if (leaf->sync_flag)
    return;
  uint64_t key, value;
  data_node_t *oldnode = leaf->data;
  if (leaf->log == NULL)
  {
    leaf->print_node();
    leaf->data->log->print_node();
    assert(false);
  }
  entry_key_t split_key = leaf->log->high_key;
  data_node_t *node[2];
  int len[2];
  int idx[2];
  int c;
  node[0] = leaf->log->data;
  node[1] = node[0]->next;
  len[0] = leaf->log->number;
  len[1] = leaf->log->next->number;
  idx[0] = idx[1] = 0;

  for (int i = 0; i < LEAF_NODE_SIZE; i++)
  {
    uint64_t key = leaf->data->kv[i].key;
    if (key != 0 && leaf->check_slot(i))
    {
      if (key < split_key)
        c = 0;
      else
        c = 1;
      while (idx[c] < len[c])
      {
        // Find the first Valid key-value
        if (!node[c]->kv[idx[c]].key)
          break;
        idx[c]++;
      }
      // If syncLeaf has Valid key-value
      if (idx[c] < len[c])
      {
        if (node[c]->kv[idx[c]].key == key) // Match the key
        {
          // printf("match!\n");
          // If the value mismatch and has not sync yet: do synchrozing
          value = uint64_t(leaf->data->kv[i].ptr) & (~MASK);
          uint64_t v = uint64_t(node[c]->kv[idx[c]].ptr);
          if ((v & (COPY_MASK)) != 0 && (v & (~MASK)) != value)
            __sync_val_compare_and_swap(&node[c]->kv[idx[c]].ptr, v, (char *)(value | SYNC_MASK));
          // Move to next candidate
          idx[c]++;
        }
        else // Mismatch
        {
          // A thread delete the key in old leaf and not sync yet
          // A thread Insert the key before split begin and not sync yet (already filtered by bitmap)
          // printf("delete!\n");
          if (leaf->sync_flag)
            return;
          if (leaf->data->kv[i].key) // The key is not deleted
            // Sync the delete and move to next candidate
            node[c]->kv[idx[c]++].key = 0;
        }
      }
    }
  }
  // A thread delete the last few keys in old leaf
  // Delete it
  for (int i = idx[0]; i < len[0]; i++)
    node[0]->kv[i].key = 0;
  for (int i = idx[1]; i < len[1]; i++)
    node[1]->kv[i].key = 0;
  leaf->sync_flag = true;
  asm_mfence();
}

void btree::update_prev_node(leaf_node_t *leaf, inner_node_t *parent, leaf_node_t *prev)
{
  if (leaf->prev_flag)
    return;
  entry_key_t key = leaf->log->high_key;
  // leaf_node_t *prev;
  leaf_node_t *next = leaf->log;
  int pre_error = 0;
  int help = 0;

  // 1. find previous leaf
  if (prev == NULL)
  {
    if (!check_pred(key, (char **)&prev, parent))
      find_pred_leaf(key, (char **)&prev, (inner_node_t **)&parent);
  }

  while (true)
  {
    // 2. check the previous leaf
    if (prev != NULL)
    {
      // previous node is changed
      if ((leaf_node_t *)(prev->next) != leaf && (leaf_node_t *)(prev->next) != next)
      {
        if (leaf->prev_flag)
          return;
        // find a wrong previous node
        if (!check_pred(key, (char **)&prev, parent))
        {
          // parent is changed
          find_pred_leaf(key, (char **)&prev, (inner_node_t **)&parent);
        }
        continue;
      }
    }

    // 3. update the next pointer of previous leaf
    if (prev == NULL)
    {
      // first update the data pointer, then update the meta data pointer
      __sync_val_compare_and_swap(&data_anchor, leaf->data, next->data);
      __sync_val_compare_and_swap(&anchor, leaf, next);
    }
    else
    {
      __sync_val_compare_and_swap(&prev->data->next, leaf->data, next->data);
      __sync_val_compare_and_swap(&prev->next, leaf, next);
      // 4. help the previous leaf complete SMO
      if (!leaf->prev_flag)
      {
        if (prev->check_split())
        {
          // the parent possibly not true
          SplitLeaf(prev, parent);
          if (!leaf->prev_flag)
          {
            if (!check_pred(key, (char **)&prev, parent))
            {
              // parent is changed
              find_pred_leaf(key, (char **)&prev, (inner_node_t **)&parent);
            }
            continue;
          }
        }
      }
    }
    leaf->prev_flag = 1;
    asm_mfence();
    break;
  }
}

void btree::update_parent(leaf_node_t *leaf, inner_node_t *parent)
{
  if (leaf->fin_flag)
    return;
  leaf_node_t *firleaf = leaf->log;
  leaf_node_t *secleaf = (leaf_node_t *)(firleaf->next);
  entry_key_t splitKey = leaf->log->high_key;
  // Set a new root or insert the split key to the parent
  if (height == 1)
  { // only one node can update the root ptr
    printf("Root height:2!\n");
    inner_node_t *new_root = new inner_node_t((page *)firleaf, splitKey, (page *)secleaf, 1);
    setNewRoot((char *)new_root, leaf);
    // printf("root:%p\n", root);
  }
  else if (!check_parent((char *)firleaf, splitKey, (char *)secleaf, 1, parent, leaf))
    btree_insert_internal((char *)firleaf, splitKey, (char *)secleaf, 1, leaf);
}

bool btree::modify(leaf_node_t *leaf, int pos, entry_key_t key, char *right)
{
  leaf->data->kv[pos].ptr = right;
#ifndef eADR
  flush_data(&leaf->data->kv[pos].ptr, sizeof(uint64_t));
#else
  asm_mfence();
#endif

  return true;
}

char *btree::search(entry_key_t key)
{
  leaf_node_t *leaf;
  int pos;
  char *res;
  bool retry;

  leaf = inner_node_search(key);
  assert(key < leaf->high_key);
  assert(key >= leaf->low_key);

  uint8_t hash = hashfunc(key);
  pos = find_item(key, leaf, hash);
  if (pos == -1)
    res = NULL;
  else
    res = leaf->data->kv[pos].ptr;

  if (leaf->check_split())
  {
    // result is valid
    if (leaf->data->log == NULL)
    {
      return res;
    }
    else
    {
      leaf_node_t *new_leaf = (leaf_node_t *)(leaf->log);
      char *new_res;
      if (key >= new_leaf->high_key)
        new_leaf = (leaf_node_t *)(new_leaf->next);
      assert(key >= new_leaf->low_key);
      assert(key < new_leaf->high_key);
      pos = find_item(key, new_leaf, hash);
      if (pos == -1)
        return NULL;
      if (leaf->sync_flag)
        return leaf->data->kv[pos].ptr;
      if (res == NULL)
      {
        // item delete in old leaf, but appear in new leaf, delete it
        leaf->data->kv[pos].key = 0;
        asm_mfence();
        return res;
      }
      else
      {
        new_res = leaf->data->kv[pos].ptr;
        if (new_res == res)
          return res;
        else
        {
          leaf->data->kv[pos].ptr = res;
          asm_mfence();
          return res;
        }
      }
    }
  }
  return res;
}

bool btree::update(entry_key_t key, char *right)
{
  leaf_node_t *leaf;
  int pos;
  char *res;
  bool retry;

  leaf = inner_node_search(key);
  leaf->lock();
  assert(key < leaf->high_key);
  assert(key >= leaf->low_key);

  uint8_t hash = hashfunc(key);
  pos = find_item(key, leaf, hash);
  if (pos == -1)
    return false;
  leaf->data->kv[pos].ptr = right;
#ifndef eADR
  flush_data(&leaf->data->kv[pos].ptr, sizeof(uint64_t));
#else
  asm_mfence();
#endif
  leaf->unlock();

  return true;
}

bool btree::insert(entry_key_t key, char *right)
{
  leaf_node_t *leaf, *prev = NULL;
  inner_node_t *parent;
  int old_slot;
  uint8_t hash;
  uint8_t pos;

  // // 1. Inner node search
  // leaf = inner_node_search(key, (char **)&prev, (inner_node_t **)&parent);
  // assert(leaf != NULL);

  while (true)
  {
    // 1. Inner node search
    leaf = inner_node_search(key, (char **)&prev, (inner_node_t **)&parent);
    assert(leaf != NULL);
    
    leaf->lock();
    // 2. Conditional Check
    hash = hashfunc(key);
    old_slot = find_item(key, leaf, hash);
    if (old_slot >= 0)
    {
      bool result = modify(leaf, old_slot, key, right);
      leaf->unlock();
      return result;
    }

    // 3. Test Full
    if (leaf->number >= LEAF_NODE_SIZE)
    {
      leaf->set_split_bit();
      SplitLeaf(leaf, parent, prev, key);
      leaf->unlock();
      continue;
    }
    // 4. Allocate the pos
    pos = __sync_fetch_and_add(&leaf->number, 1);

    // 5. insert the entry
    leaf->data->kv[pos].ptr = right;
    leaf->data->kv[pos].key = key;
#ifndef eADR
    flush_data(&leaf->data->kv[pos], sizeof(entry));
#else
    asm_mfence();
#endif
    leaf->finger_prints[pos] = hash;

    // 6. Commit the insert
    bool res = leaf->set_slot(pos);
    leaf->unlock();
    break;
  }

  return true;
}

bool btree::remove(entry_key_t key)
{

  int old_slot;
  leaf_node_t *leaf;
  bool retry;
  leaf = inner_node_search(key);
  assert(key < leaf->high_key);
  assert(key >= leaf->low_key);

  // 2. traverse leaf node
  uint8_t hash = hashfunc(key);
  old_slot = find_item(key, leaf, hash);
  // printf("old slot:%d\n", old_slot);
  if (old_slot == -1)
  {
    return false;
  }

  // 3. delete the key
  leaf->data->kv[old_slot].key = 0;
#ifndef eADR
  flush_data(&leaf->data->kv[old_slot].key, sizeof(entry_key_t));
#else
  asm_mfence();
#endif
  while (leaf->check_split())
  {
    if (leaf->data->log == NULL)
    {
      return true;
    }
    else
    {
      leaf_node_t *new_leaf = (leaf_node_t *)(leaf->log);
      if (key >= new_leaf->high_key)
        new_leaf = (leaf_node_t *)(new_leaf->next);
      assert(key >= new_leaf->low_key);
      assert(key < new_leaf->high_key);
      old_slot = find_item(key, new_leaf, hash);
      if (old_slot == -1)
        return true;
      // Prevent from delete the new insert
      new_leaf->data->kv[old_slot].key = 0;
      asm_mfence();
      leaf = new_leaf;
    }
  }
  return true;
}
