//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) { }

// O(n), n equals to replacer_size_
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
  // frame_id is nullptr
  // if (frame_id != nullptr) {
  //   return false;
  // }
  
  // the buffer has no element

  if (fifo_dl_.empty() && lru_dl_.empty()) {
    return false; 
  }

  if (evictable_cnt_fifo_ > 0) {
    auto iter = fifo_dl_.rbegin();
    while(iter != fifo_dl_.rend()) {
      if (iter->is_evictable_) {
        if (frame_id != nullptr) {
          *frame_id = iter->fid_;
        }
        node_store_.erase(iter->fid_);
        fifo_dl_.erase(--iter.base());
        --evictable_cnt_fifo_;
        --curr_size_;
        return true;
      }
      iter++;
    }
  }

  if (evictable_cnt_lru_ > 0) {
    auto iter = lru_dl_.rbegin();
    
    while(iter != lru_dl_.rend()) {
      if (iter->is_evictable_) {
        if (frame_id != nullptr) {
          *frame_id = iter->fid_;
        }
        node_store_.erase(iter->fid_);
        lru_dl_.erase(--iter.base());
        --evictable_cnt_lru_;
        --curr_size_;
        return true;
      }
      iter++;
    }
  }

  return false;
  // can O(1) to evict a page?
}

// O(?)
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {

  if (static_cast<size_t>(frame_id) > replacer_size_ or frame_id < 0) {
    throw Exception("invalid frame_id");
  }

  // it no need to evict
  current_timestamp_++;
  auto it = node_store_.find(frame_id);

  if (it != node_store_.end()) {
    auto &target = it->second;
    if (target->k_ + 1 < k_) {
      fifo_dl_.push_front(*target);
      fifo_dl_.erase(target);
      target = fifo_dl_.begin();
      target->k_++;
      target->history_.push_back(current_timestamp_);
    } else {
      target->k_++;
      target->history_.push_back(current_timestamp_);

      auto pos = lru_dl_.begin();

      if (target->k_ > k_) {
        target->history_.pop_front();
      }

      while(pos != lru_dl_.end()) {
        if (*pos->history_.begin() < *target->history_.begin()) {
          break;
        }
        pos++;
      }

      auto iter = lru_dl_.insert(pos, *target);
      
      if(target->k_ == k_) {
        if (target->is_evictable_) {
          evictable_cnt_lru_++;
          evictable_cnt_fifo_--;
        } 
        fifo_dl_.erase(target);
      } else if(target->k_ > k_) {
        lru_dl_.erase(target);
      }
      node_store_[frame_id] = iter;

    }

    return;
  } 

  if (curr_size_ == replacer_size_) {
    Evict(nullptr);
    RecordAccess(frame_id);
    return;
  }

  fifo_dl_.push_front(LRUKNode(current_timestamp_, frame_id));
  node_store_.insert({frame_id, fifo_dl_.begin()});
  curr_size_++;
}

// O(1)
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto it = node_store_.find(frame_id);

  if (it == node_store_.end()) {
    return;
  }

  auto &target = it->second;

  if (target->is_evictable_ == set_evictable) {
    return;
  }

  target->is_evictable_ = set_evictable;
  if (target->k_ < k_) {
    evictable_cnt_fifo_ += set_evictable? 1: -1;
  } else {
    evictable_cnt_lru_ += set_evictable? 1: -1;
  }
}

// O(1)
void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  auto &target = it->second;

  if (!target->is_evictable_) {
    throw Exception("the removed frame is non-evictable");
  }

  if (target->k_ < k_) {
    fifo_dl_.erase(target);
    evictable_cnt_fifo_--;
  } else {
    lru_dl_.erase(target);
    evictable_cnt_lru_--;
  }

  node_store_.erase(it);
  curr_size_--;
}

// O(1)
auto LRUKReplacer::Size() -> size_t { 
  return evictable_cnt_fifo_ + evictable_cnt_lru_; 
}

}  // namespace bustub
