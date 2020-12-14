// Amanda Barana and Olivia Grimes

// This file implements a bundle as a linked list of bundle entries.
// This bundle linked list is specifically designed to be used by LevelDB.

#ifndef BUNDLE_LINKED_BUNDLE_H
#define BUNDLE_LINKED_BUNDLE_H

#include <pthread.h>
#include <sys/types.h>

#include <atomic>
#include <mutex>

#include "common_bundle.h"
#include "plaf.h"
#include "rq_debugging.h"

#define CPU_RELAX asm volatile("pause\n" ::: "memory")

#define DEBUG_PRINT(str)                         \
  if ((i + 1) % 10000 == 0) {                    \
    std::cout << str << std::endl << std::flush; \
  }                                              \
  ++i;

enum op { NOP, INSERT, REMOVE };

template <typename NodeType>
class BundleEntry : public BundleEntryBase<NodeType> {
 public:
  volatile timestamp_t ts_;  // Redefinition of ts_ to make it volitile.

  // Additional members.
  BundleEntry *volatile next_;
  volatile timestamp_t deleted_ts_;

  explicit BundleEntry(timestamp_t ts, NodeType *ptr, BundleEntry *next)
      : ts_(ts), next_(next) {
    this->ptr_ = ptr;
    deleted_ts_ = BUNDLE_NULL_TIMESTAMP;
  }

  void set_ts(const timestamp_t ts) { ts_ = ts; }
  void set_ptr(NodeType *const ptr) { this->ptr_ = ptr; }
  void set_next(BundleEntry *const next) { next_ = next; }
  void mark(timestamp_t ts) { deleted_ts_ = ts; }
  timestamp_t marked() { return deleted_ts_; }

  inline void validate() {
    if (ts_ < next_->ts_) {
      std::cout << "Invalid bundle" << std::endl;
      exit(1);
    }
  }
};

template <typename NodeType>
class Bundle : public BundleInterface<NodeType> {
 private:
  std::atomic<BundleEntry<NodeType> *> head_;
  BundleEntry<NodeType> *volatile tail_;
#ifdef BUNDLE_DEBUG
  volatile int updates = 0;
  BundleEntry<NodeType> *volatile last_recycled = nullptr;
  volatile int oldest_edge = 0;
#endif

 public:
  ~Bundle() {
    BundleEntry<NodeType> *curr = head_;
    BundleEntry<NodeType> *next;
    while (curr != tail_) {
      next = curr->next_;
      delete curr;
      curr = next;
    }
    delete tail_;
  }

  void init() override {
    tail_ = new BundleEntry<NodeType>(BUNDLE_NULL_TIMESTAMP, nullptr, nullptr);
    head_ = tail_;
  }

  // Inserts a new rq_bundle_node at the head of the bundle.
  inline void prepare(NodeType *const ptr) override {
    BundleEntry<NodeType> *new_entry =
        new BundleEntry<NodeType>(BUNDLE_PENDING_TIMESTAMP, ptr, nullptr);
    BundleEntry<NodeType> *expected;
    while (true) {
      expected = head_;
      new_entry->next_ = expected;
      long i = 0;
      while (expected->ts_ == BUNDLE_PENDING_TIMESTAMP) {
        // DEBUG_PRINT("insertAtHead");
        CPU_RELAX;
      }
      if (head_.compare_exchange_weak(expected, new_entry)) {
#ifdef BUNDLE_DEBUG
        ++updates;
#endif
        return;
      }
    }
  }

  // Labels the pending entry to make it visible to range queries.
  inline void finalize(timestamp_t ts) override {
    BundleEntry<NodeType> *entry = head_;
    assert(entry->ts_ == BUNDLE_PENDING_TIMESTAMP);
    entry->ts_ = ts;
  }

  // Returns a reference to the node that immediately followed at timestamp ts.
  inline NodeType *getPtrByTimestamp(timestamp_t ts) override {
    // Start at head and work backwards until edge is found.
    BundleEntry<NodeType> *curr = head_;
    long i = 0;
    while (curr->ts_ == BUNDLE_PENDING_TIMESTAMP) {
      // DEBUG_PRINT("getPtrByTimestamp");
      CPU_RELAX;
    }
    while (curr != tail_ && curr->ts_ > ts) {
      assert(curr->ts_ != BUNDLE_NULL_TIMESTAMP);
      curr = curr->next_;
    }
#ifdef BUNDLE_DEBUG
    if (curr->marked()) {
      std::cout << dump(0) << std::flush;
      exit(1);
    }
#endif
    return curr->ptr_;
  }

};

#endif  // BUNDLE_LINKED_BUNDLE_H