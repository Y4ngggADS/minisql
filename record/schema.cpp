#include "record/schema.h"

/**
 * DONE: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(SCHEMA_MAGIC_NUM));
  offset += sizeof(SCHEMA_MAGIC_NUM);
  uint32_t columnCount = columns_.size();
  memcpy(buf + offset, &columnCount, sizeof(columnCount));
  offset += sizeof(columnCount);
  for (auto column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t size = sizeof(SCHEMA_MAGIC_NUM) + sizeof(uint32_t);
  for (auto col : columns_) size += col->GetSerializedSize();
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset = 0;
  uint32_t magic_number;
  memcpy(&magic_number, buf + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);
  ASSERT(magic_number == SCHEMA_MAGIC_NUM, "DeserializeFrom for invalid schema buf");
  
  uint32_t column_count;
  memcpy(&column_count, buf + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);
  
  std::vector<Column *> columns(column_count);
  for (uint32_t i = 0; i < column_count; i++) {
    Column *column;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns[i] = column;
  }
  
  schema = new Schema(columns, true);
  return offset;
}