#include "page/bitmap_page.h"

// #include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ >= MAX_CHARS * 8) return false;
  page_offset = next_free_page_;
  bytes[page_offset / 8] |= (0x80 >> (page_offset % 8));
  page_allocated_++;
  
  for (uint32_t i = next_free_page_ + 1; i < MAX_CHARS * 8; i++) {
    if (IsPageFreeLow(i / 8, i % 8)) {
      next_free_page_ = i;
      return true;
    }
  }
  next_free_page_ = MAX_CHARS * 8;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFreeLow(page_offset / 8, page_offset % 8)) return false;
  bytes[page_offset / 8] &= ~(0x80 >> (page_offset % 8));
  page_allocated_--;
  if (page_allocated_ == MAX_CHARS * 8 - 1) next_free_page_ = page_offset;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return !(bytes[byte_index] & (0x80 >> bit_index));
}

template <size_t PageSize>
uint32_t BitmapPage<PageSize>::GetNextFreePage(){
    return next_free_page_;
};

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;