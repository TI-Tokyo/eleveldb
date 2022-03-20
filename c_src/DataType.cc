#include "DataType.h"

std::ostream&
eleveldb::operator<<(std::ostream& os, DataType::Type type)
{
        switch (type) {
        case DataType::Type::NIL:
                os << "NIL";
                break;
        case DataType::Type::BOOL:
                os << "BOOL";
                break;
        case DataType::Type::INT:
                os << "INT";
                break;
        case DataType::Type::DOUBLE:
                os << "DOUBLE";
                break;
        case DataType::Type::STRING:
                os << "STRING";
                break;
        case DataType::Type::TIMESTAMP:
                os << "TIMESTAMP";
                break;
        default:
                os << "unknown (" << (int)type << ")";
                break;
        }

        return os;
}
