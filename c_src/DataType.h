#ifndef DATATYPE_H
#define DATATYPE_H

/**
 * DataType
 *
 *   A class for managing data-type specifications
 *
 * Tagged: Wed Sep  9 11:10:10 PDT 2015
 *
 * Original author: eleitch@basho.com
 */
#include <sstream>

namespace eleveldb {

class DataType {
public:
        enum class Type {
                UNSUPPORTED,
                NIL,
                BOOL,
                INT,
                DOUBLE,
                STRING,
                TIMESTAMP,
        };

        friend std::ostream& operator<<(std::ostream&,  DataType::Type);
};

std::ostream& operator<<(std::ostream& os,  DataType::Type type);

}

#endif
