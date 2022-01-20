#ifndef ELEVELDB_EIUTIL_H
#define ELEVELDB_EIUTIL_H

/**
 * EiUtil
 *
 *   A class for operating on erlang term_to_binary-encoded data,
 *   using the ei library (see http://erlang.org/doc/man/ei.html)
 * 
 * Created: Wed Sep  9 17:32:28 PDT 2015
 * 
 * Original author: eleitch@basho.com
 */
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <vector>

#include "ei.h"

#include "DataType.h"

#define EI_CONV_UINT8_FN(fn)  uint8_t  (fn)(char* buf, int* index)
#define EI_CONV_INT64_FN(fn)  int64_t  (fn)(char* buf, int* index)
#define EI_CONV_UINT64_FN(fn) uint64_t (fn)(char* buf, int* index)
#define EI_CONV_DOUBLE_FN(fn) double   (fn)(char* buf, int* index)

#define FN_DECL(retType, name) \
        retType name();                         \
        static retType name(char* buf, int* index);

namespace eleveldb {

    class EiUtil {
    public:

        //------------------------------------------------------------
        // What I'm calling Big is a type encoded by TTB, but for
        // which no encoding is implemented in the ei library.
        // 
        // It can contain a positive or negative value, and can store
        // ints as an arbitrary number of bytes.
        //
        // This struct is for managing up to 8-byte integers encoded
        // as a big
        //------------------------------------------------------------
        
        struct Big {
            bool isSigned_;
            unsigned size_;
            uint64_t val_;

            Big() {
                isSigned_ = false;
                size_     = 0;
                val_      = 0;
            };
        };
        
        /**
         * Constructor.
         */
        EiUtil();
        EiUtil(char* buf);

        /**
         * Destructor.
         */
        virtual ~EiUtil();

        void initialize(char* buf, int index);

        FN_DECL(std::string, printTerm);
        FN_DECL(std::string, formatTerm);

        FN_DECL(int, getVersion);
        FN_DECL(int, getTupleHeader);
        FN_DECL(int, getListHeader);
        
        FN_DECL(ei_term, decodeTerm);
        
        FN_DECL(int, getType);

        // This is the size as returned by ei_get_type, which is _not_
        // the size in bytes of an object, but size in # elements
        
        FN_DECL(int, getSizeInElements);

        // This returns the size in bytes of the next object in the
        // buffer
        
        static void getSizeInBytes(char* buf, int* index, unsigned& nHeaderBytes, unsigned& nDataBytes);

        FN_DECL(bool, isInteger);
        FN_DECL(bool, isFloat);
        FN_DECL(bool, isAtom);
        FN_DECL(bool, isBool);
        FN_DECL(bool, isRef);
        FN_DECL(bool, isPort);
        FN_DECL(bool, isPid);
        FN_DECL(bool, isNil);
        FN_DECL(bool, isTuple);
        FN_DECL(bool, isString);
        FN_DECL(bool, isList);
        FN_DECL(bool, isBinary);

        FN_DECL(bool, isBig);
        FN_DECL(bool, canBeUint64);
        static bool isBig(char* buf, int* index, unsigned& size, bool& isSigned);
        
        FN_DECL(bool, isFun);
        
        static bool isInteger(int type);
        static bool isFloat(int type);
        static bool isAtom(int type);
        static bool isRef(int type);
        static bool isPort(int type);
        static bool isPid(int type);
        static bool isNil(int type);
        static bool isTuple(int type);
        static bool isString(int type);
        static bool isList(int type);
        static bool isBinary(int type);
        static bool isBig(int type);
        static bool isFun(int type);
        
        FN_DECL(std::string, typeStrOf);
        FN_DECL(DataType::Type, typeOf);

        static std::string typeStrOf(int type);
        static DataType::Type typeOf(int type, char* buf, int* index);

        static unsigned char* getDataPtr(char* buf, int* index, size_t& size, 
                                         bool includeMarker);

        FN_DECL(bool,          getBool);
        FN_DECL(std::string,   getAtom);
        FN_DECL(std::string,   getString);
        FN_DECL(double,        getDouble);
        FN_DECL(double,        getFloat);
        FN_DECL(long,          getLong);
        FN_DECL(unsigned long, getUlong);
        FN_DECL(int64_t,       getInt64);
        FN_DECL(uint64_t,      getUint64);
        FN_DECL(Big,           getBig);
        FN_DECL(uint64_t,      getBigAsUint64);
        FN_DECL(std::vector<unsigned, char> getBinary);
        FN_DECL(std::string,   getBinaryAsString);
        FN_DECL(std::string,   getBinaryAsStringEml);
        
        FN_DECL(std::string, formatAtom);
        FN_DECL(std::string, formatInteger);
        FN_DECL(std::string, formatBig);
        FN_DECL(std::string, formatFloat);
        FN_DECL(std::string, formatTuple);
        FN_DECL(std::string, formatBinary);
        FN_DECL(std::string, formatString);
        FN_DECL(std::string, formatList);

        FN_DECL(void, skipTuple);
        FN_DECL(void, skipList);
        FN_DECL(void, skipLastReadObject);

        static void skipNext(char* buf, int* index, unsigned int& nHeaderBytes, unsigned int& nDataBytes);

        std::map<std::string, DataType::Type> parseMap();
        static std::map<std::string, DataType::Type> parseMap(char* buf, int* index);

        // Print a map of field/type pairs

        static void printMap(std::map<std::string, DataType::Type>& keyValMap);

        // Return a map of conversion functions

        static std::map<DataType::Type, EI_CONV_UINT8_FN(*)>  constructUint8Map();
        static std::map<DataType::Type, EI_CONV_INT64_FN(*)>  constructInt64Map();
        static std::map<DataType::Type, EI_CONV_UINT64_FN(*)> constructUint64Map();
        static std::map<DataType::Type, EI_CONV_DOUBLE_FN(*)> constructDoubleMap();

        // Static maps of conversion functions

        static std::map<DataType::Type, EI_CONV_UINT8_FN(*)>  uint8ConvMap_;
        static std::map<DataType::Type, EI_CONV_INT64_FN(*)>  int64ConvMap_;
        static std::map<DataType::Type, EI_CONV_UINT64_FN(*)> uint64ConvMap_;
        static std::map<DataType::Type, EI_CONV_DOUBLE_FN(*)> doubleConvMap_;

        // Object conversion functions

        static uint8_t  objectToUint8(char* buf, int* index);
        static int64_t  objectToInt64(char* buf, int* index);
        static uint64_t objectToUint64(char* buf, int* index);
        static double   objectToDouble(char* buf, int* index);
        
        // Templatized type-conversion functions

        template<typename typeTo, typename typeFrom> 
            static typeTo convert(char* buf, int* index);

    private:

        void checkBuf();

        char* buf_;
        int index_;
        int version_;

    }; // End class EiUtil
    
} // End namespace eleveldb

#endif // End #ifndefELEVELDB_EIUTIL_H
