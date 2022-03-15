#include "CmpUtil.h"
#include "EiUtil.h"
#include "ErlUtil.h"
#include "StringBuf.h"
#include "exceptionutils.h"
#include "extractor.h"
#include "filter_parser.h"
#include "workitems.h"

#include <msgpack.hpp>

using namespace eleveldb;

//=======================================================================
// Methods of Extractor base class
//=======================================================================

Extractor::Extractor()
{
    typesParsed_    = false;
    nField_         = 0;
}

Extractor::~Extractor() {}

/**.......................................................................
 * Add a field to the map of expression fields.  If a type was
 * specified as ANY with the filter, store the original specified type
 * in the map.  Else store the (possibly converted to a different
 * supported type) inferred type from the data structure.
 *
 * This will be used later to distinguish binaries that were specified
 * as binaries from opaque items that are treated as binaries because
 * we don't know what they are (type 'any').
 */
void Extractor::add_field(std::string field)
{
    if(expr_field_specs_.find(field) != expr_field_specs_.end()) {

        if(expr_field_specs_[field] == DataType::ANY) {
            expr_fields_[field] = expr_field_specs_[field];
        } else {
            expr_fields_[field] = cTypeOf(field);
        }

    } else {
        expr_fields_[field] = cTypeOf(field);
    }

    nField_ = expr_fields_.size();
}

/**.......................................................................
 * Convert from a Time-Series type-specifier to a DataType enum
 */
DataType::Type Extractor::tsAtomToType(std::string tsAtom, bool throwIfInvalid)
{
    DataType::Type type = DataType::UNKNOWN;

    // Used to be 'binary', now it's 'varchar'

    if(tsAtom == "varchar") {
        type = DataType::STRING;

        // Used to be 'integer', now it's 'sint64'

    } else if(tsAtom == "sint64") {
        type = DataType::INT;

        // Used to be 'float', now it's 'double'

    } else if(tsAtom == "double") {
        type = DataType::DOUBLE;

    } else if(tsAtom == "boolean") {
        type = DataType::BOOL;
    } else if(tsAtom == "timestamp") {
        type = DataType::TIMESTAMP;
    } else if(tsAtom == "any") {
        type = DataType::ANY;
    } else {
        if(throwIfInvalid) {
            ThrowRuntimeError("Unsupported data type: '" << tsAtom << "'");
        }
    }

    return type;
}

/**.......................................................................
 * Convert from a Time-Series type-specifier to a supported C-style DataType
 */
DataType::Type Extractor::tsAtomToCtype(std::string tsAtom, bool throwIfInvalid)
{
    return convertToSupportedCType(tsAtomToType(tsAtom, throwIfInvalid));
}

/**.......................................................................
 * Return the data type of the requested field.
 */
DataType::Type Extractor::cTypeOf(std::string fieldName)
{
    //------------------------------------------------------------
    // If we haven't already parsed a value, we don't know what type
    // this field is
    //------------------------------------------------------------

    if(!typesParsed_)
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // If the field wasn't found, we don't know what type this field is
    //------------------------------------------------------------

    if(field_types_.find(fieldName) == field_types_.end())
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // Else retrieve the stored type, potentially converting to a
    // supported type if this is not a native type we support
    //------------------------------------------------------------

    DataType::Type type = field_types_[fieldName];
    return convertToSupportedCType(type);
}

/**.......................................................................
 * Convert a field of type 'type' to one of the C-types we support
 */
DataType::Type Extractor::convertToSupportedCType(DataType::Type type)
{
        //------------------------------------------------------------
        // Else try to upcast to a supported type.
        //------------------------------------------------------------

        switch (type) {

                //------------------------------------------------------------
                // Basic types are supported
                //------------------------------------------------------------

        case DataType::DOUBLE:
        case DataType::STRING:
        case DataType::INT:
        case DataType::UINT:
        case DataType::BOOL:
                return type;

                //------------------------------------------------------------
                // Until clarified, timestamps are treated as uint64_t
                //------------------------------------------------------------

        case DataType::TIMESTAMP:
                return DataType::UINT;

                //------------------------------------------------------------
                // All other types are treated as opaque binaries
                //------------------------------------------------------------

        default:
                ThrowRuntimeError("Refusing to convert type " << type);
        }
}

void Extractor::printMap(std::map<std::string, DataType::Type>& keyTypeMap)
{
        for(auto iter : keyTypeMap) {
                COUT("'" << iter.first << "' " << convertToSupportedCType(iter.second));
                FOUT("'" << iter.first << "' " << convertToSupportedCType(iter.second));
        }
}

/**.......................................................................
 * Return the data type of the operands to a binary operator
 */
DataType::Type Extractor::cTypeOf(ErlNifEnv* env, ERL_NIF_TERM oper1, ERL_NIF_TERM oper2, bool throwIfInvalid)
{
    DataType::Type type1 = cTypeOf(env, oper1, throwIfInvalid);
    DataType::Type type2 = cTypeOf(env, oper2, throwIfInvalid);

    //------------------------------------------------------------
    // If both are const expressions, default to double comparisons
    //------------------------------------------------------------

    if(type1 == DataType::CONST && type2 == DataType::CONST)
        return DataType::DOUBLE;

    //------------------------------------------------------------
    // If either is unknown, return unknown -- we can't compare anything
    //------------------------------------------------------------

    else if(type1 == DataType::UNKNOWN || type2 == DataType::UNKNOWN)
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // If both are non-const, then they had better match for comparisons
    //------------------------------------------------------------

    else if(type1 != DataType::CONST && type2 != DataType::CONST && (type1 != type2))
        return DataType::UNKNOWN;

    //------------------------------------------------------------
    // Else return whichever type is non-const
    //------------------------------------------------------------

    else
        return type1 == DataType::CONST ? type2 : type1;
}

/**.......................................................................
 * Parse the field name out of a tuple, and return its type.
 *
 * Old-style tuples were of the form:
 *
 *    {field, "fieldname"} or {const, val}
 *
 * New-style tuples should be of the form:
 *
 *    {field, "fieldname", type} or {const, val}
 *
 */
DataType::Type Extractor::cTypeOf(ErlNifEnv* env, ERL_NIF_TERM tuple, bool throwIfInvalid)
{
    int arity=0;
    const ERL_NIF_TERM* op_args=0;

    //------------------------------------------------------------
    // ErlUtil::get methods will throw if the args are not the correct
    // type.  Capture this and return UNKNOWN if the tuple is
    // malformed
    //------------------------------------------------------------

    try {
        if (enif_get_tuple(env, tuple, &arity, &op_args)) {

            std::string op = ErlUtil::getAtom(env, op_args[0]);
            std::string fieldName;

            if (!(op == eleveldb::filter::FIELD_OP || op == eleveldb::filter::CONST_OP)) {
                if(throwIfInvalid)
                    ThrowRuntimeError("Invalid operand type: '" << op << "' while parsing expression: '"
                                      << ErlUtil::formatTerm(env, tuple) << "'");
            }

            if(op == eleveldb::filter::FIELD_OP)
                fieldName = ErlUtil::getBinaryAsString(env, op_args[1]);

            //------------------------------------------------------------
            // Check 2-tuples
            //------------------------------------------------------------

            if(arity == 2) {

                // If this is a constant expression, we defer to the field value
                // against which we will be comparing it

                if(op == eleveldb::filter::CONST_OP)
                    return DataType::CONST;

                // Else a field -- parse the field name, and return the type of
                // the datum for that field

                if(op == eleveldb::filter::FIELD_OP)
                    return cTypeOf(fieldName);

            //------------------------------------------------------------
            // Check 3-tuples
            //------------------------------------------------------------

            } else if(arity == 3) {

                // If this is a constant expression, and a type has
                // been specified, we still defer to the field value
                // against which we will be comparing it

                if(op == eleveldb::filter::CONST_OP) {
                    return DataType::CONST;
                } else {
                    std::string type = ErlUtil::getAtom(env, op_args[2]);

                    // Store the type as-specified

                    expr_field_specs_[fieldName] = tsAtomToType(type, throwIfInvalid);

                    // Overwrite any inferences we may have made from parsing the data

                    DataType::Type specType = tsAtomToCtype(type, throwIfInvalid);

                    field_types_[fieldName] = specType;

                    // NB: Now that we are not inferring data types
                    // from the decoded data, set expr_fields_ to the
                    // explicit type, for use during data extraction

                    expr_fields_[fieldName] = specType;

                    // And return the type

                    return tsAtomToCtype(type, throwIfInvalid);
                }
            }
        }

        if(throwIfInvalid)
            ThrowRuntimeError("Invalid field or const specifier: " << ErlUtil::formatTerm(env, tuple));

    } catch(std::runtime_error& err) {
        if(throwIfInvalid)
            throw err;
    }

    return DataType::UNKNOWN;
}

/**.......................................................................
 * Given the start of a key data binary, return the start and size of the
 * contents portion of an encoded riak object
 */
void Extractor::seekToRiakObjectContents(const char* data, size_t size,
                                         const char** contentsPtr, size_t* contentsSize)
{
        const char* ptr = data;

        //------------------------------------------------------------
        // Skip the magic number and version
        //------------------------------------------------------------

        unsigned char magic    = (*ptr++);
        unsigned char vers     = (*ptr++);

        if(!(magic == 53 && vers == 1))
                ThrowRuntimeError("Riak object contents can only be inspected for magic = 53 and v1 encoding");

        //------------------------------------------------------------
        // Skip the vclock len and vclock contents
        //------------------------------------------------------------

        unsigned int vClockLen = ntohl(*((unsigned int*)ptr));
        ptr += 4;
        ptr += vClockLen;

        unsigned char encMagic = (*ptr++);

        //------------------------------------------------------------
        // The next byte should now be the msgpack magic number (2).
        // Check that it is
        //------------------------------------------------------------

        if(!(encMagic == MSGPACK_MAGIC || encMagic == ERLANG_MAGIC))
                ThrowRuntimeError("This record uses an unsupported encoding");

        //------------------------------------------------------------
        // Skip the sibling count
        //------------------------------------------------------------

        unsigned int sibCount =  ntohl(*((unsigned int*)ptr));
        ptr += 4;

        if(sibCount != 1)
                ThrowRuntimeError("Unexpected sibling count for time-series data: " << sibCount);

        //------------------------------------------------------------
        // Now we are on to the first (and only) sibling.  Get the length of
        // the data contents for this sibling
        //------------------------------------------------------------

        unsigned int valLen =  ntohl(*((unsigned int*)ptr));
        ptr += 4;

        //------------------------------------------------------------
        // Set the passed ptr pointing to the start of the contents for this
        // object, and set the returned length to be just the length of the
        // contents
        //------------------------------------------------------------

        ptr++;  // TypeTag
        ptr++;  // extra encoding marker

        *contentsPtr  = ptr;
        *contentsSize = valLen;
}


//=======================================================================
// Methods of Msgpack extractor
//=======================================================================

void ExtractorMsgpack::extract(const char* data, size_t size, ExpressionNode<bool>* root)
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
                        root->set_value(key, (void*)&v, DataType::BOOL);
                }
                break;
                case msgpack::type::NEGATIVE_INTEGER:
                case msgpack::type::POSITIVE_INTEGER:
                {
                        int64_t v;
                        oi.convert(v);
                        root->set_value(key, (void*)&v, DataType::INT);
                }
                break;
                case msgpack::type::FLOAT32:
                {
                        float v;
                        oi.convert(v);
                        double vi (v);
                        root->set_value(key, (void*)&vi, DataType::DOUBLE);
                }
                break;
                case msgpack::type::FLOAT64:
                {
                        double v;
                        oi.convert(v);
                        root->set_value(key, (void*)&v, DataType::DOUBLE);
                }
                break;
                case msgpack::type::STR:
                {
                        std::string v;
                        oi.convert(v);
                        root->set_value(key, (void*)&v, DataType::STRING);
                }
                break;
                default:
                        ThrowRuntimeError("Unhandled type in TS record: " << oi.type);
                }
                ++ki;
        }
}

/**.......................................................................
 * Read through a msgpack-encoded object, parsing the data types for
 * each field we encounter
 */
void ExtractorMsgpack::parseTypes(const char* data, size_t size)
{
        field_types_ = std::move(CmpUtil::parseMap(data, size));
}
