#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : frame_iters(num_pages, lru_list.end()) {
  Capacity = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list.empty()) return false;
  *frame_id = lru_list.back();
  frame_iters[*frame_id] = lru_list.end();
  lru_list.pop_back();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if (auto it = frame_iters[frame_id]; it != lru_list.end()) {
    lru_list.erase(it);
    frame_iters[frame_id] = lru_list.end();
  }
}  

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list.size() >= Capacity || frame_iters[frame_id] != lru_list.end()) return;
  lru_list.push_front(frame_id);
  frame_iters[frame_id] = lru_list.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() { 
  return lru_list.size(); 
}