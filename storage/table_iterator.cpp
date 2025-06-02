#include "storage/table_iterator.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), rid_(rid), row_(rid), txn_(txn), is_end_(false) {
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    is_end_ = true;
    return;
  }
  bool found = table_heap_->GetTuple(&row_, txn_);
  if (!found) {
    TryGetNextValidRow();
  }
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_), 
      rid_(other.rid_), 
      row_(other.rid_), 
      txn_(other.txn_), 
      is_end_(other.is_end_) {
  if (!is_end_) {
    table_heap_->GetTuple(&row_, txn_);
  }
}

TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
  if (is_end_ && itr.is_end_) {
    return true;
  }
  if (is_end_ || itr.is_end_) {
    return false;
  }
  return rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(!is_end_, "TableIterator::operator* - Iterator is at end!");
  return row_;
}

Row *TableIterator::operator->() {
  ASSERT(!is_end_, "TableIterator::operator-> - Iterator is at end!");
  return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    rid_ = itr.rid_;
    row_ = Row(rid_);
    txn_ = itr.txn_;
    is_end_ = itr.is_end_;
    if (!is_end_) {
      table_heap_->GetTuple(&row_, txn_);
    }
  }
  return *this;
}

TableIterator &TableIterator::operator++() {
  if (is_end_) {
    return *this;
  }
  if (!TryGetNextValidRow()) {
    is_end_ = true;
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator temp = *this;
  ++(*this);
  return temp;
}

bool TableIterator::TryGetNextValidRow() {
  page_id_t current_page_id = rid_.GetPageId();
  while (current_page_id != INVALID_PAGE_ID) {
    auto table_page = reinterpret_cast<TablePage*>(
        table_heap_->buffer_pool_manager_->FetchPage(current_page_id));
    if (table_page == nullptr) {
      return false;
    }
    RowId next_rid;
    bool found = table_page->GetNextTupleRid(rid_, &next_rid);
    table_heap_->buffer_pool_manager_->UnpinPage(current_page_id, false);
    
    if (found) {
      rid_ = next_rid;
      row_.SetRowId(rid_);
      if (table_heap_->GetTuple(&row_, txn_)) {
        return true;
      }
    } 
    else {
      current_page_id = table_page->GetNextPageId();
      rid_ = RowId(current_page_id, 0); 
    }
  }
  return false;
}