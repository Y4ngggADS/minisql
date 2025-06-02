#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t o = 0;
  memcpy(buf+o, &COLUMN_MAGIC_NUM, sizeof(COLUMN_MAGIC_NUM)); o += sizeof(COLUMN_MAGIC_NUM);
  size_t l = name_.length();
  memcpy(buf+o, &l, sizeof(l)); o += sizeof(l);
  memcpy(buf+o, name_.c_str(), l); o += l;
  memcpy(buf+o, &type_, sizeof(type_)); o += sizeof(type_);
  memcpy(buf+o, &len_, sizeof(len_)); o += sizeof(len_);
  memcpy(buf+o, &table_ind_, sizeof(table_ind_)); o += sizeof(table_ind_);
  memcpy(buf+o, &nullable_, sizeof(nullable_)); o += sizeof(nullable_);
  memcpy(buf+o, &unique_, sizeof(unique_)); o += sizeof(unique_);
  return o;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return sizeof(COLUMN_MAGIC_NUM) + 
         sizeof(size_t) + name_.length() + 
         sizeof(type_) + sizeof(len_) + 
         sizeof(table_ind_) + 
         sizeof(nullable_) + sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t offset = 0;
  uint32_t magic_number = MACH_READ_INT32(buf + offset); offset += sizeof(uint32_t);
  ASSERT(magic_number == COLUMN_MAGIC_NUM, "column deserialize wrong, not a colum buffer");
  size_t name_len = MACH_READ_FROM(size_t, buf + offset); offset += sizeof(size_t);
  std::string name(buf + offset, name_len); offset += name_len;
  TypeId type; memcpy(&type, buf + offset, sizeof(type)); offset += sizeof(type);
  uint32_t length = MACH_READ_INT32(buf + offset); offset += sizeof(uint32_t);
  uint32_t index = MACH_READ_INT32(buf + offset); offset += sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool, buf + offset); offset += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf + offset); offset += sizeof(bool);
  column = type == kTypeChar ?
    new Column(name, type, length, index, nullable, unique) :
    new Column(name, type, index, nullable, unique);
  return offset;
}  