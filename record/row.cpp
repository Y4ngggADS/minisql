#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  const uint32_t field_count = fields_.size();
  memcpy(buf + offset, &field_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  const uint32_t bitmap_size = (field_count + 7) / 8;
  char *bitmap = buf + offset;
  memset(bitmap, 0, bitmap_size);
  offset += bitmap_size;
  for (uint32_t i = 0; i < field_count; i++) {
    if (fields_[i]->IsNull()) {
      bitmap[i / 8] |= (1 << (i % 8));
    } 
    else {
      offset += fields_[i]->SerializeTo(buf + offset);
    }
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t field_count;
  memcpy(&field_count, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  const uint32_t bitmap_size = (field_count + 7) / 8;
  const char *bitmap = buf + offset;
  offset += bitmap_size;
  fields_.resize(field_count);
  for (uint32_t i = 0; i < field_count; i++) {
    const bool is_null = bitmap[i / 8] & (1 << (i % 8));
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumns()[i]->GetType(), &fields_[i], is_null);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = sizeof(uint32_t);
  size += (fields_.size() + 7) / 8;
  for (auto field : fields_) {
    if (!field->IsNull()) {
      size += field->GetSerializedSize();
    }
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}