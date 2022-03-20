#include "CmpUtil.h"
#include "EiUtil.h"
#include "ErlUtil.h"
#include "StringBuf.h"
#include "exceptionutils.h"
#include "extractor.h"
#include "filter_parser.h"
#include "workitems.h"

#include <msgpack.hpp>

#include <fstream>
#include <ios>

using namespace eleveldb;

//=======================================================================
// Methods of Extractor base class
//=======================================================================

void
Extractor::parseTypes(ErlNifEnv* env, const ERL_NIF_TERM& options)
{
        std::vector<ERL_NIF_TERM> opArgs = ErlUtil::getListCells(env, options);

        size_t ki = 1;
        for (auto& ai : opArgs) {
                const std::string key = std::to_string(ki++);
                std::string ts_type = ErlUtil::getAtom(env, ai);
                field_types_[key] = tsAtomToCtype(ts_type);
        }
}

/**.......................................................................
 * Convert from a Time-Series type-specifier to a DataType enum
 */
DataType::Type
Extractor::tsAtomToType(const std::string& t)
{
        // Used to be 'binary', now it's 'varchar'
        if (t == "varchar")
                return DataType::Type::STRING;
        else if (t == "sint64")
                return DataType::Type::INT;
        else if (t == "double")
                return DataType::Type::DOUBLE;
        else if (t == "boolean")
                return DataType::Type::BOOL;
        else if (t == "timestamp")
                return DataType::Type::TIMESTAMP;
        else
                ThrowRuntimeError("Unsupported data type: '" << t << "'");
}


/**.......................................................................
 * Return the data type of the requested field.
 */
DataType::Type
Extractor::cTypeOf(const std::string& fieldName) const
{
        auto found = field_types_.find(fieldName);
        if (found == field_types_.end())
                return DataType::Type::UNSUPPORTED;

        return convertToSupportedCType(found->second);
}

/**.......................................................................
 * Convert a field of type 'type' to one of the C-types we support
 */
DataType::Type
Extractor::convertToSupportedCType(const DataType::Type type)
{
        switch (type) {

        case DataType::Type::DOUBLE:
        case DataType::Type::STRING:
        case DataType::Type::INT:
        case DataType::Type::BOOL:
                return type;

        case DataType::Type::TIMESTAMP:
                return DataType::Type::INT;

        default:
                ThrowRuntimeError("Refusing to convert type " << type);
        }
}

/**.......................................................................
 * Parse the field name out of a tuple, and return its type.
 *
 *    {field, "fieldname"} or {const, val}
 *
 */
DataType::Type
Extractor::cTypeOf(ErlNifEnv* env, const ERL_NIF_TERM& tuple) const
{
        int _arity;
        const ERL_NIF_TERM* op_args;

        if (enif_get_tuple(env, tuple, &_arity, &op_args)) {

                std::string op = ErlUtil::getAtom(env, op_args[0]);

                if (op == eleveldb::filter::CONST_OP)
                        return ErlUtil::typeOf(env, op_args[1]);

                if (op == eleveldb::filter::FIELD_OP) {
                        auto ret = cTypeOf(ErlUtil::getBinaryAsString(env, op_args[1]));
                        return ret;
                }

                // else, it's a binary operator that evaluates to boolean
                return DataType::Type::BOOL;
        }

        ThrowRuntimeError("Invalid field or const specifier: " << ErlUtil::formatTerm(env, tuple));
}


/**.......................................................................
 * Given the start of a key data binary, return the start and size of the
 * contents portion of an encoded riak object
 */
void
Extractor::seekToRiakObjectContents(const char* data, size_t size,
                                    const char** contentsPtr, size_t* contentsSize)
{
        const char* ptr = data;

        // Skip the magic number and version
        unsigned char magic = (*ptr++);
        unsigned char vers  = (*ptr++);

        if (!(magic == 53 && vers == 1))
                ThrowRuntimeError("Riak object contents can only be inspected for magic = 53 and v1 encoding");

        // Skip the vclock len and vclock contents
        unsigned int vClockLen = ntohl(*((unsigned int*)ptr));
        ptr += 4;
        ptr += vClockLen;

        unsigned char encMagic = (*ptr++);

        // The next byte should now be the msgpack magic number (2).
        // Check that it is
        if (!(encMagic == MSGPACK_MAGIC || encMagic == ERLANG_MAGIC))
                ThrowRuntimeError("This record uses an unsupported encoding");

        // Skip the sibling count
        unsigned int sibCount =  ntohl(*(unsigned int*)ptr);
        ptr += 4;

        if (sibCount != 1)
                ThrowRuntimeError("Unexpected sibling count for time-series data: " << sibCount);

        // Now we are on to the first (and only) sibling. Get the length of
        // the data contents for this sibling
        unsigned int valLen =  ntohl(*((unsigned int*)ptr));
        ptr += 4;

        // Set the passed ptr pointing to the start of the contents for this
        // object, and set the returned length to be just the length of the
        // contents
        ptr++;  // TypeTag
        ptr++;  // extra encoding marker

        *contentsPtr  = ptr;
        *contentsSize = valLen;
}


void
ExtractorMsgpack::extract(const char* data, size_t size, ExpressionNode<bool>* root)
{
        auto oh = msgpack::unpack(data, size);
        auto obj = oh.get();
        std::vector<msgpack::object> oa;
        obj.convert(oa);
        size_t ki = 1;
        for (auto& oi : oa) {
                std::string key = std::to_string(ki);
                switch (oi.type) {
                case msgpack::type::BOOLEAN:
                {
                        bool v;
                        oi.convert(v);
                        root->set_value(key, &v, DataType::Type::BOOL);
                }
                break;
                case msgpack::type::NEGATIVE_INTEGER:
                case msgpack::type::POSITIVE_INTEGER:
                {
                        int64_t v;
                        oi.convert(v);
                        root->set_value(key, &v, DataType::Type::INT);
                }
                break;
                case msgpack::type::FLOAT32:
                {
                        float v;
                        oi.convert(v);
                        double vi (v);
                        root->set_value(key, &vi, DataType::Type::DOUBLE);
                }
                break;
                case msgpack::type::FLOAT64:
                {
                        double v;
                        oi.convert(v);
                        root->set_value(key, &v, DataType::Type::DOUBLE);
                }
                break;
                case msgpack::type::STR:
                {
                        std::string v;
                        oi.convert(v);
                        root->set_value(key, &v, DataType::Type::STRING);
                }
                break;
                case msgpack::type::ARRAY:
                {
                        int dummy = 0;
                        root->set_value(key, &dummy, DataType::Type::NIL);
                }
                break;
                default:
                        ThrowRuntimeError("Unhandled type in TS record: " << oi.type);
                }
                ++ki;
        }
}
