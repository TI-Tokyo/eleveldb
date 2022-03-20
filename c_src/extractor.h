#ifndef EXTRACTOR_H
#define EXTRACTOR_H

#include <stdint.h>
#include <map>
#include <string>

#include "filter.h"
#include "cmp_mem_access.h"

#include "erl_nif.h"

#include "DataType.h"
#include "Encoding.h"

//=======================================================================
// A base class for extracting data
//=======================================================================

class Extractor {
public:
        Extractor() {}
        virtual ~Extractor() {}

        // Get the DataType::Type corresponding to the ts atom
        static eleveldb::DataType::Type tsAtomToType(const std::string&);

        // Convert from ts atom to supported C-type
        static eleveldb::DataType::Type tsAtomToCtype(const std::string& type)
                {
                        return convertToSupportedCType(
                                tsAtomToType(type));
                }

        eleveldb::DataType::Type cTypeOf(ErlNifEnv*,
                                         const ERL_NIF_TERM& operand) const;

        eleveldb::DataType::Type cTypeOf(const std::string& field) const;

        void parseTypes(ErlNifEnv*, const ERL_NIF_TERM&);

        virtual void extract(const char *data, size_t size, ExpressionNode<bool>* root) = 0;

        static void seekToRiakObjectContents(const char* data, size_t size,
                                             const char** contentsPtr, size_t* contentsSize);

        static DataType::Type convertToSupportedCType(DataType::Type type);

        std::map<std::string, eleveldb::DataType::Type> field_types_;
};

//=======================================================================
// A base class for extracting data encoded in msgpack format
//=======================================================================

class ExtractorMsgpack : public Extractor {
private:
        cmp_ctx_t cmp_;

public:
        ExtractorMsgpack() {}
        virtual ~ExtractorMsgpack() {}

        void extract(const char *data, size_t size, ExpressionNode<bool>* root);

        void setBinaryVal(ExpressionNode<bool>* root, std::string& key,
                          cmp_mem_access_t* ma, cmp_ctx_t* cmp, cmp_object_t* obj, bool includeMarker);

        void setStringVal(ExpressionNode<bool>* root, std::string& key,
                          cmp_object_t* obj);
};


#endif
