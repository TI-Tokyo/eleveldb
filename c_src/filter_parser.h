#ifndef INCL_FILTER_PARSER_H
#define INCL_FILTER_PARSER_H

#include "erl_nif.h"
#include "filter.h"
#include "extractor.h"

namespace eleveldb {

namespace filter {
static const char* const EQ_OP    = "=";
static const char* const EQEQ_OP  = "==";
static const char* const NEQ_OP   = "!=";
static const char* const LT_OP    = "<";
static const char* const GT_OP    = ">";
static const char* const LTE_OP   = "<=";
static const char* const ELT_OP   = "=<";
static const char* const GTE_OP   = ">=";

static const char* const AND_OP   = "and";
static const char* const AND__OP  = "and_";
static const char* const OR_OP    = "or";
static const char* const OR__OP   = "or_";
static const char* const FIELD_OP = "field";
static const char* const CONST_OP = "const";
}

//=======================================================================
// Top-level call to parse range-filter
//=======================================================================
ExpressionNode<bool>* parse_range_filter_opts(ErlNifEnv*, const ERL_NIF_TERM& options, const Extractor&);

}


#endif
