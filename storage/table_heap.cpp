#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    auto page_id = first_page_id_;
    while (page_id != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if (page == nullptr) {
            return false;
        }
        page->WLatch();
        if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, true);
            return true;
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = page->GetNextPageId();
    }

    // If no available page found, create a new page
    page_id_t new_page_id; 
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id)); // 直接传递变量
    if (new_page == nullptr) {
        return false;
    }
    page_id = new_page_id; 
    new_page->Init(page_id, first_page_id_ == INVALID_PAGE_ID ? INVALID_PAGE_ID : first_page_id_, log_manager_, txn);
    if (first_page_id_ != INVALID_PAGE_ID) {
        auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
        while (last_page->GetNextPageId() != INVALID_PAGE_ID) {
            auto next_page_id = last_page->GetNextPageId();
            buffer_pool_manager_->UnpinPage(last_page->GetTablePageId(), false);
            last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
        }
        last_page->SetNextPageId(page_id);
        buffer_pool_manager_->UnpinPage(last_page->GetTablePageId(), true);
    } 
    else {
        first_page_id_ = page_id;
    }
    new_page->WLatch();
    bool result = new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, result);
    return result;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the recovery.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }
    Row old_row(rid);
    page->WLatch();
    bool result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), result);
    return result;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    // Step1: Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Step2: Delete the tuple from the page.
    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) {
        return false;
    }
    page->RLatch();
    bool result = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
            DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } 
    else {
        DeleteTable(first_page_id_);
    }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
    RowId first_rid;
    auto page_id = first_page_id_;
    while (page_id != INVALID_PAGE_ID) {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if (page->GetFirstTupleRid(&first_rid)) {
            buffer_pool_manager_->UnpinPage(page_id, false);
            return TableIterator(this, first_rid, txn);
        }
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = page->GetNextPageId();
    }
    return End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);
}