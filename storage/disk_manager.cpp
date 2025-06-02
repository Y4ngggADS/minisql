#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  // ASSERT(false, "Not implemented yet.");
  uint32_t metadata[PAGE_SIZE/4];
  memcpy(metadata, meta_data_, PAGE_SIZE);

  uint32_t extentId = 0;
  while (metadata[2 + extentId] == BITMAP_SIZE) extentId++;

  char bitmap[PAGE_SIZE];
  page_id_t bitmapPageId = extentId * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(bitmapPageId, bitmap);

  auto* bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);
  uint32_t freePage = bitmapPage->GetNextFreePage();
  page_id_t pageId = extentId * BITMAP_SIZE + freePage;

  bitmapPage->AllocatePage(freePage);
  if (extentId >= metadata[1]) metadata[1]++;
  metadata[2 + extentId]++;
  metadata[0]++;

  memcpy(meta_data_, metadata, PAGE_SIZE);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmapPageId, bitmap);
  return pageId;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logicalPageId) {
  // ASSERT(false, "Not implemented yet.");
  char bitmap[PAGE_SIZE];
  uint32_t extentId = logicalPageId / BITMAP_SIZE;
  page_id_t bitmapPageId = 1 + extentId * (BITMAP_SIZE + 1);
  ReadPhysicalPage(bitmapPageId, bitmap);
  auto* bitmapPage = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap);

  bitmapPage->DeAllocatePage(logicalPageId % BITMAP_SIZE);

  uint32_t metadata[PAGE_SIZE/4];
  memcpy(metadata, meta_data_, PAGE_SIZE);
  if (--metadata[2 + extentId] == 0) metadata[1]--;
  metadata[0]--;

  memcpy(meta_data_, metadata, PAGE_SIZE);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmapPageId, bitmap);
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bm[PAGE_SIZE];
  page_id_t eid = logical_page_id / BITMAP_SIZE;
  page_id_t bm_pid = 1 + eid * (BITMAP_SIZE + 1);
  ReadPhysicalPage(bm_pid, bm);
  auto* bm_page = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bm);
  return bm_page->IsPageFree(logical_page_id % BITMAP_SIZE);
}  

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id / BITMAP_SIZE + logical_page_id + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}