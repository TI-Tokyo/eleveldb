#include "DataType.h"

using namespace std;

using namespace eleveldb;

/**.......................................................................
 * Constructor.
 */
DataType::DataType() {}

/**.......................................................................
 * Destructor.
 */
DataType::~DataType() {}

ostream&
eleveldb::operator<<(ostream& os, DataType::Type type)
{
  switch (type) {
  case DataType::ANY:
      os << "ANY";
      break;
  case DataType::UNHANDLED:
      os << "UNHANDLED";
      break;
  case DataType::NIL:
      os << "NIL";
      break;
  case DataType::ARRAY:
      os << "ARRAY";
      break;
  case DataType::BOOL:
      os << "BOOL";
      break;
  case DataType::CONST:
      os << "CONST";
      break;
  case DataType::CHAR:
      os << "CHAR";
      break;
  case DataType::EXT:
      os << "EXT";
      break;
  case DataType::INT:
      os << "INT";
      break;
  case DataType::UINT:
      os << "UINT";
      break;
  case DataType::DOUBLE:
      os << "DOUBLE";
      break;
  case DataType::STRING:
      os << "STRING";
      break;
  case DataType::MAP:
      os << "MAP";
      break;
  case DataType::TIMESTAMP:
      os << "TIMESTAMP";
      break;
  default:
      os << "UNKNOWN";
      break;
  }

  return os;
}
