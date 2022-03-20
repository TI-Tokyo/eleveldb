#include "CmpUtil.h"
#include "StringBuf.h"
#include "exceptionutils.h"

#include <msgpack.hpp>

#include <sstream>
#include <map>

using namespace std;

using namespace eleveldb;

//=======================================================================
// A macro for declaring a template convert specialization
//=======================================================================

#define CONVERT_DECL(typeTo, typeFrom, cmpType, validation)             \
    namespace eleveldb {                                                \
        template<>                                                      \
        typeTo CmpUtil::convert<typeTo, typeFrom>(cmp_object_t* obj)    \
        {                                                               \
            typeFrom val=0;                                             \
            if(cmp_object_as_##cmpType(obj, &val)) {                    \
                validation;                                             \
            }                                                           \
                                                                        \
            ThrowRuntimeError("Object of type " << CmpUtil::typeStrOf(obj) \
                              << " can't be represented as a " << #typeTo); \
            return (typeTo)val;                                         \
        }                                                               \
    }\


/**.......................................................................
 * Return the type of this object
 */
DataType::Type CmpUtil::typeOf(cmp_object_t* obj)
{
        switch (obj->type) {
        case CMP_TYPE_FIXSTR:
        case CMP_TYPE_STR8:
        case CMP_TYPE_STR16:
        case CMP_TYPE_STR32:
                return DataType::Type::STRING;
                break;
        case CMP_TYPE_NIL:
                return DataType::Type::NIL;
                break;
        case CMP_TYPE_BOOLEAN:
                return DataType::Type::BOOL;
                break;
        case CMP_TYPE_BIN8:
        case CMP_TYPE_BIN16:
        case CMP_TYPE_BIN32:
                return DataType::Type::STRING;
                break;
        case CMP_TYPE_FLOAT:
        case CMP_TYPE_DOUBLE:
                return DataType::Type::DOUBLE;
                break;

        case CMP_TYPE_POSITIVE_FIXNUM:
        case CMP_TYPE_UINT8:
        case CMP_TYPE_UINT16:
        case CMP_TYPE_UINT32:
        case CMP_TYPE_UINT64:
        case CMP_TYPE_NEGATIVE_FIXNUM:
        case CMP_TYPE_SINT8:
        case CMP_TYPE_SINT16:
        case CMP_TYPE_SINT32:
        case CMP_TYPE_SINT64:
                return DataType::Type::INT;
                break;

        case CMP_TYPE_FIXMAP:
        case CMP_TYPE_MAP16:
        case CMP_TYPE_MAP32:
        case CMP_TYPE_FIXARRAY:
        case CMP_TYPE_ARRAY16:
        case CMP_TYPE_ARRAY32:
        case CMP_TYPE_FIXEXT1:
        case CMP_TYPE_FIXEXT2:
        case CMP_TYPE_FIXEXT4:
        case CMP_TYPE_FIXEXT8:
        case CMP_TYPE_FIXEXT16:
        case CMP_TYPE_EXT8:
        case CMP_TYPE_EXT16:
        case CMP_TYPE_EXT32:
        default:
                return DataType::Type::UNSUPPORTED;
        }
}

/**.......................................................................
 * Return a string-ified version of the type of this object
 */
std::string CmpUtil::typeStrOf(cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_POSITIVE_FIXNUM:
        return "CMP_TYPE_POSITIVE_FIXNUM";
        break;
    case CMP_TYPE_UINT8:
        return "CMP_TYPE_UINT8";
        break;
    case CMP_TYPE_NEGATIVE_FIXNUM:
        return "CMP_TYPE_NEGATIVE_FIXNUM";
        break;
    case CMP_TYPE_SINT8:
        return "CMP_TYPE_SINT8";
        break;
    case CMP_TYPE_FIXMAP:
        return "CMP_TYPE_FIXMAP";
        break;
    case CMP_TYPE_MAP16:
        return "CMP_TYPE_MAP16";
        break;
    case CMP_TYPE_MAP32:
        return "CMP_TYPE_MAP32";
        break;
    case CMP_TYPE_FIXARRAY:
        return "CMP_TYPE_FIXARRAY";
        break;
    case CMP_TYPE_ARRAY16:
        return "CMP_TYPE_ARRAY16";
        break;
    case CMP_TYPE_ARRAY32:
        return "CMP_TYPE_ARRAY32";
        break;
    case CMP_TYPE_FIXSTR:
        return "CMP_TYPE_FIXSTR";
        break;
    case CMP_TYPE_STR8:
        return "CMP_TYPE_STR8";
        break;
    case CMP_TYPE_STR16:
        return "CMP_TYPE_STR16";
        break;
    case CMP_TYPE_STR32:
        return "CMP_TYPE_STR32";
        break;
    case CMP_TYPE_NIL:
        return "CMP_TYPE_NIL";
        break;
    case CMP_TYPE_BOOLEAN:
        return "CMP_TYPE_BOOLEAN";
        break;
    case CMP_TYPE_BIN8:
        return "CMP_TYPE_BIN8";
        break;
    case CMP_TYPE_BIN16:
        return "CMP_TYPE_BIN16";
        break;
    case CMP_TYPE_BIN32:
        return "CMP_TYPE_BIN32";
        break;
    case CMP_TYPE_FIXEXT1:
        return "CMP_TYPE_FIXEXT1";
        break;
    case CMP_TYPE_FIXEXT2:
        return "CMP_TYPE_FIXEXT2";
        break;
    case CMP_TYPE_FIXEXT4:
        return "CMP_TYPE_FIXEXT4";
        break;
    case CMP_TYPE_FIXEXT8:
        return "CMP_TYPE_FIXEXT8";
        break;
    case CMP_TYPE_FIXEXT16:
        return "CMP_TYPE_FIXEXT16";
        break;
    case CMP_TYPE_EXT8:
        return "CMP_TYPE_EXT8";
        break;
    case CMP_TYPE_EXT16:
        return "CMP_TYPE_EXT16";
        break;
    case CMP_TYPE_EXT32:
        return "CMP_TYPE_EXT32";
        break;
    case CMP_TYPE_FLOAT:
        return "CMP_TYPE_FLOAT";
        break;
    case CMP_TYPE_DOUBLE:
        return "CMP_TYPE_DOUBLE";
        break;
    case CMP_TYPE_UINT16:
        return "CMP_TYPE_UINT16";
        break;
    case CMP_TYPE_SINT16:
        return "CMP_TYPE_SINT16";
        break;
    case CMP_TYPE_UINT32:
        return "CMP_TYPE_UINT32";
        break;
    case CMP_TYPE_SINT32:
        return "CMP_TYPE_SINT32";
        break;
    case CMP_TYPE_UINT64:
        return "CMP_TYPE_UINT64";
        break;
    case CMP_TYPE_SINT64:
        return "CMP_TYPE_SINT64";
        break;
    default:
        return "UNKNOWN";
        break;
    }
}

/**.......................................................................
 * Return the size, in bytes, of the passed object.
 *
 * NB: After a call to this function, the memory pointer will be
 * advanced to the end of the current object.
 */
size_t CmpUtil::dataSizeOf(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_POSITIVE_FIXNUM:
    case CMP_TYPE_NEGATIVE_FIXNUM:
    case CMP_TYPE_UINT8:
    case CMP_TYPE_SINT8:
        return sizeof(int8_t);
        break;
    case CMP_TYPE_FIXMAP:
    case CMP_TYPE_MAP16:
    case CMP_TYPE_MAP32:
        return mapSize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXARRAY:
    case CMP_TYPE_ARRAY16:
    case CMP_TYPE_ARRAY32:
        return arraySize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXSTR:
    case CMP_TYPE_STR8:
    case CMP_TYPE_STR16:
    case CMP_TYPE_STR32:
        return stringSize(ma, cmp, obj);
        break;

    // These types are markers only (no data)

    case CMP_TYPE_NIL:
        return 0;
        break;
    case CMP_TYPE_BOOLEAN:
        return 0;
        break;
    case CMP_TYPE_BIN8:
    case CMP_TYPE_BIN16:
    case CMP_TYPE_BIN32:
        return binarySize(ma, cmp, obj);
        break;
    case CMP_TYPE_FIXEXT1:
    case CMP_TYPE_FIXEXT2:
    case CMP_TYPE_FIXEXT4:
    case CMP_TYPE_FIXEXT8:
    case CMP_TYPE_FIXEXT16:
    case CMP_TYPE_EXT8:
    case CMP_TYPE_EXT16:
    case CMP_TYPE_EXT32:
        ThrowRuntimeError("Unhandled type: EXT");
        break;
    case CMP_TYPE_FLOAT:
        return sizeof(float);
        break;
    case CMP_TYPE_DOUBLE:
        return sizeof(double);
        break;
    case CMP_TYPE_UINT16:
        return sizeof(uint16_t);
        break;
    case CMP_TYPE_SINT16:
        return sizeof(int16_t);
        break;
    case CMP_TYPE_UINT32:
        return sizeof(uint32_t);
        break;
    case CMP_TYPE_SINT32:
        return sizeof(int32_t);
        break;
    case CMP_TYPE_UINT64:
        return sizeof(uint64_t);
        break;
    case CMP_TYPE_SINT64:
        return sizeof(int64_t);
        break;
    default:
        ThrowRuntimeError("Can't determine a size for an unsupported type");
        break;
    }
}

/**.......................................................................
 * Return the message-pack marker size
 */
size_t CmpUtil::markerSize()
{
    return sizeof(uint8_t);
}

/**.......................................................................
 * Return the size of any prefix data pre-pended to this object's data
 */
size_t CmpUtil::prefixSizeOf(cmp_object_t* obj)
{
    switch (obj->type) {
    case CMP_TYPE_BIN8:
    case CMP_TYPE_STR8:
        return sizeof(uint8_t);
        break;
    case CMP_TYPE_ARRAY16:
    case CMP_TYPE_BIN16:
    case CMP_TYPE_MAP16:
    case CMP_TYPE_STR16:
        return sizeof(uint16_t);
        break;
    case CMP_TYPE_ARRAY32:
    case CMP_TYPE_BIN32:
    case CMP_TYPE_MAP32:
    case CMP_TYPE_STR32:
        return sizeof(uint32_t);
        break;
    case CMP_TYPE_FIXEXT1:
    case CMP_TYPE_FIXEXT2:
    case CMP_TYPE_FIXEXT4:
    case CMP_TYPE_FIXEXT8:
    case CMP_TYPE_FIXEXT16:
    case CMP_TYPE_EXT8:
    case CMP_TYPE_EXT16:
    case CMP_TYPE_EXT32:
        ThrowRuntimeError("Unhandled type: EXT");
        break;
    default:
        return 0;
        break;
    }
}

/**.......................................................................
 * Return the size of a map object
 */
size_t CmpUtil::mapSize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* map)
{
    size_t size = 0;

    uint32_t map_size=0;
    if(!cmp_object_as_map(map, &map_size))
      ThrowRuntimeError("Unable to parse data as a msgpack map");

    //------------------------------------------------------------
    // Iterate over the map, which consists of pairs of objects
    //------------------------------------------------------------

    for(unsigned int i=0; i < map_size; i++) {

        //------------------------------------------------------------
        // Read each object in the pair
        //------------------------------------------------------------

        cmp_object_t obj;

        if(cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read first object in element "
                              << i << "of the map");

        size += markerSize() + prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);

        if(!cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read second object in element "
                              << i << "of the map");

        size += markerSize() + prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);
    }

    return size;
}

/**.......................................................................
 * Return the size of an array object
 */
size_t CmpUtil::arraySize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* arr)
{
    size_t size = 0;

    uint32_t arr_size=0;
    if(!cmp_object_as_array(arr, &arr_size))
      ThrowRuntimeError("Unable to parse data as a msgpack arr");

    //------------------------------------------------------------
    // Iterate over the arr, which consists of pairs of objects
    //------------------------------------------------------------

    for(unsigned int i=0; i < arr_size; i++) {

        //------------------------------------------------------------
        // Read each object in turn
        //------------------------------------------------------------

        cmp_object_t obj;

        if(!cmp_read_object(cmp, &obj))
            ThrowRuntimeError("Failed to read element " << i << "of the array");

        // Array data consists of sequential data of the same type --
        // no embedded markers, as with a map

        size += prefixSizeOf(&obj) + dataSizeOf(ma, cmp, &obj);
    }

    return size;
}

/**.......................................................................
 * Return the size of a msgpack-encoded string.  The pointer is
 * repositioned at the end of the binary, so that all size operations
 * have uniform behavior
 */
size_t CmpUtil::stringSize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* str)
{
    uint32_t str_size=0;
    if(!cmp_object_as_str(str, &str_size))
      ThrowRuntimeError("Unable to parse data as a msgpack string");

    cmp_mem_access_set_pos(ma, cmp_mem_access_get_pos(ma) + str_size);

    return str_size;
}

/**.......................................................................
 * Return the size of a msgpack-encoded binary.  The pointer is
 * repositioned at the end of the binary, so that all size operations
 * have uniform behavior
 */
size_t CmpUtil::binarySize(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* bin)
{
    uint32_t bin_size=0;
    if(!cmp_object_as_bin(bin, &bin_size))
      ThrowRuntimeError("Unable to parse data as a msgpack binary");

    cmp_mem_access_set_pos(ma, cmp_mem_access_get_pos(ma) + bin_size);

    return bin_size;
}

/**.......................................................................
 * Skip over the last read object.  Just reads the size, since that
 * operation has the side effect of moving the pointer to the end of
 * the object
 */
void CmpUtil::skipLastReadObject(cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj)
{
    dataSizeOf(ma, cmp, obj);
}

/**.......................................................................
 * Print a map of keys + datatypes
 */
void CmpUtil::printMap(std::map<std::string, DataType::Type>& keyValMap)
{
    for(std::map<std::string, DataType::Type>::iterator iter = keyValMap.begin();
        iter != keyValMap.end(); iter++) {
        COUT(iter->first << " " << iter->second);
    }
}

/**.......................................................................
 * Return a pointer to the data for this object, and its size
 */
unsigned char* CmpUtil::getDataPtr(cmp_mem_access_t* ma, cmp_ctx_t* cmp,
                                   cmp_object_t* obj, size_t& size, bool includeMarker)
{
    //------------------------------------------------------------
    // Store the current location of the pointer before querying the
    // size, since that can in principle advance the pointer
    //------------------------------------------------------------

    unsigned char* ptr =
        (unsigned char*)cmp_mem_access_get_ptr_at_pos(ma, cmp_mem_access_get_pos(ma));

    //------------------------------------------------------------
    // Now query the size, which will advance the pointer
    //------------------------------------------------------------

    size = dataSizeOf(ma, cmp, obj) + (includeMarker ? 1 : 0);

    return ptr - (includeMarker ? 1 : 0);
}

/**.......................................................................
 * Return true if this field is empty -- i.e., [], used to signify
 * null value from riak_kv
 */
bool CmpUtil::isEmptyList(cmp_object_t* obj)
{
    if(obj->type != CMP_TYPE_FIXARRAY)
        return false;

    uint32_t size=0;
    if(!cmp_object_as_array(obj, &size))
        return false;

    return size == 0;
}

//=======================================================================
// Templatized convert specializations
//=======================================================================

//------------------------------------------------------------
// Conversions to uint8_t
//------------------------------------------------------------

CONVERT_DECL(uint8_t, bool, bool,
             return val;
    );

CONVERT_DECL(uint8_t, uint8_t, uchar,
             return val;
    );

CONVERT_DECL(uint8_t, int8_t, char,
             if(val >= 0)
                 return val;
    );

CONVERT_DECL(uint8_t, int16_t, short,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint16_t, ushort,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int32_t, int,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint32_t, uint,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, int64_t, long,
             if(val >= 0 && val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, uint64_t, ulong,
             if(val <= UCHAR_MAX)
                 return val;
    );

CONVERT_DECL(uint8_t, float, float,
             if(val >= 0.0 && val <= (float)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

CONVERT_DECL(uint8_t, double, double,
             if(val >= 0.0 && val <= (double)UCHAR_MAX && !(fabs(val - (uint8_t)val) > 0.0))
                 return val;
    );

// //------------------------------------------------------------
// // Conversions to int64_t
// //------------------------------------------------------------

// CONVERT_DECL(int64_t, bool, bool,
//              return val;
//     );

// CONVERT_DECL(int64_t, uint8_t, uchar,
//              return val;
//     );

// CONVERT_DECL(int64_t, int8_t, char,
//              return val;
//     );

// CONVERT_DECL(int64_t, int16_t, short,
//              return val;
//     );

// CONVERT_DECL(int64_t, uint16_t, ushort,
//              return val;
//     );

// CONVERT_DECL(int64_t, int32_t, int,
//              return val;
//     );

// CONVERT_DECL(int64_t, uint32_t, uint,
//              return val;
//     );

// CONVERT_DECL(int64_t, int64_t, long,
//              return val;
//     );

// CONVERT_DECL(int64_t, uint64_t, ulong,
//              if(val <= LLONG_MAX)
//                  return val;
//     );

// CONVERT_DECL(int64_t, float, float,
//              if(val <= (float)LLONG_MAX && val >= (float)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
//                  return val;
//     );

// CONVERT_DECL(int64_t, double, double,
//              if(val <= (double)LLONG_MAX && val >= (double)LLONG_MIN && !(fabs(val - (int64_t)val) > 0.0))
//                  return val;
//     );


// //------------------------------------------------------------
// // Conversions to uint64_t
// //------------------------------------------------------------

// CONVERT_DECL(uint64_t, bool, bool,
//              return val;
//     );

// CONVERT_DECL(uint64_t, uint8_t, uchar,
//              return val;
//     );

// CONVERT_DECL(uint64_t, int8_t, char,
//              if(val >= 0)
//                  return val;
//     );

// CONVERT_DECL(uint64_t, int16_t, short,
//              if(val >= 0)
//                  return val;
//     );

// CONVERT_DECL(uint64_t, uint16_t, ushort,
//              return val;
//     );

// CONVERT_DECL(uint64_t, int32_t, int,
//              if(val >= 0)
//                  return val;
//     );

// CONVERT_DECL(uint64_t, uint32_t, uint,
//              return val;
//     );

// CONVERT_DECL(uint64_t, int64_t, long,
//              if(val >= 0)
//                  return val;
//     );

// CONVERT_DECL(uint64_t, uint64_t, ulong,
//              return val;
//     );

// CONVERT_DECL(uint64_t, float, float,
//              if(val >= 0.0 && val <= (float)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
//                  return val;
//     );

// CONVERT_DECL(uint64_t, double, double,
//              if(val >= 0.0 && val <= (double)ULONG_MAX && !(fabs(val - (uint64_t)val) > 0.0))
//                  return val;
//     );

// //------------------------------------------------------------
// // Conversions to double
// //------------------------------------------------------------

// CONVERT_DECL(double, bool, bool,
//              return val;
//     );

// CONVERT_DECL(double, uint8_t, uchar,
//              return val;
//     );

// CONVERT_DECL(double, int8_t, char,
//              return val;
//     );

// CONVERT_DECL(double, int16_t, short,
//              return val;
//     );

// CONVERT_DECL(double, uint16_t, ushort,
//              return val;
//     );

// CONVERT_DECL(double, int32_t, int,
//              return val;
//     );

// CONVERT_DECL(double, uint32_t, uint,
//              return val;
//     );

// CONVERT_DECL(double, int64_t, long,
//              return val;
//     );

// CONVERT_DECL(double, uint64_t, ulong,
//              return val;
//     );

// CONVERT_DECL(double, float, float,
//              return val;
//     );

// CONVERT_DECL(double, double, double,
//                  return val;
//     );


// uint8_t CmpUtil::objectToUint8(cmp_object_t* obj)
// {
//     if(CmpUtil::uint8ConvMap_.find(obj->type) != CmpUtil::uint8ConvMap_.end())
//         return CmpUtil::uint8ConvMap_[obj->type](obj);
//     else
//         ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a uint8_t type");
//     return 0;
// }

// int64_t CmpUtil::objectToInt64(cmp_object_t* obj)
// {
//     if(CmpUtil::int64ConvMap_.find(obj->type) != CmpUtil::int64ConvMap_.end())
//         return CmpUtil::int64ConvMap_[obj->type](obj);
//     else
//         ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to an int64_t type");
//     return 0;
// }

// uint64_t CmpUtil::objectToUint64(cmp_object_t* obj)
// {
//     if(CmpUtil::uint64ConvMap_.find(obj->type) != CmpUtil::uint64ConvMap_.end())
//         return CmpUtil::uint64ConvMap_[obj->type](obj);
//     else
//         ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a uint64_t type");
//     return 0;
// }

// double CmpUtil::objectToDouble(cmp_object_t* obj)
// {
//     if(CmpUtil::doubleConvMap_.find(obj->type) != CmpUtil::doubleConvMap_.end())
//         return CmpUtil::doubleConvMap_[obj->type](obj);
//     else
//         ThrowRuntimeError("Object of type " << typeStrOf(obj) << " can't be converted to a double type");
//     return 0.0;
// }

// std::map<uint8_t, CONV_UINT8_FN(*)>  CmpUtil::constructUint8Map()
// {
//     std::map<uint8_t, CONV_UINT8_FN(*)> convMap;
//     CONSTRUCT_CONV_MAP(uint8_t);
//     return convMap;
// }

// std::map<uint8_t, CONV_INT64_FN(*)>  CmpUtil::constructInt64Map()
// {
//     std::map<uint8_t, CONV_INT64_FN(*)> convMap;
//     CONSTRUCT_CONV_MAP(int64_t);
//     return convMap;
// }

// std::map<uint8_t, CONV_UINT64_FN(*)>  CmpUtil::constructUint64Map()
// {
//     std::map<uint8_t, CONV_UINT64_FN(*)> convMap;
//     CONSTRUCT_CONV_MAP(uint64_t);
//     return convMap;
// }

// std::map<uint8_t, CONV_DOUBLE_FN(*)>  CmpUtil::constructDoubleMap()
// {
//     std::map<uint8_t, CONV_DOUBLE_FN(*)> convMap;
//     CONSTRUCT_CONV_MAP(double);
//     return convMap;
// }
