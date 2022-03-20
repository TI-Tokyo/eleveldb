#include "filter_parser.h"

#include "DataType.h"
#include "ErlUtil.h"
#include <ios>

//------------------------------------------------------------
// Get a new binary operator of the requested type.  If either the
// left or right expressions don't evaulate to a valid expression,
// return nullptr for the binary operator.  This will have the effect of
// ignoring the condition in the filter evaluation.
//
// This can happen, for example, if someone tries to pass a filter
// like:
//
//      {"<", [{field, "field1"}, {const, 0}]},
//
// where 'field1' refers to a string field.  (in this case, we will
// try to inspect the value 0 as a binary, which will fail since it is
// sent to the NIF as an integer,
//------------------------------------------------------------
// -- no, this can't happen because of sanity checks in Erlang code

namespace {

template<typename T>
void* parse_expression_node(ErlNifEnv*, const ERL_NIF_TERM& root, const Extractor&);

template<typename T>
ExpressionNode<T>* parse_field_expr(ErlNifEnv*, const ERL_NIF_TERM&);

template<typename T>
ExpressionNode<T>* parse_const_expr(ErlNifEnv*, const ERL_NIF_TERM&);

template <typename C, typename T>
void*
make_biary_op_of_type(const std::vector<ERL_NIF_TERM>& args,
                      ErlNifEnv* env, const Extractor& ext)
{
        auto l = (ExpressionNode<T>*)parse_expression_node<T>(env, args[1], ext);
        auto r = (ExpressionNode<T>*)parse_expression_node<T>(env, args[2], ext);

        return new C(l, r);
}

//=======================================================================
// Templates for expression parsing
//=======================================================================

template<>
ExpressionNode<bool>*
parse_const_expr(ErlNifEnv* env, const ERL_NIF_TERM& operand)
{
        bool val = eleveldb::ErlUtil::getValAsBoolean(env, operand);
        return new ConstantValue<bool>(val);
}

template<>
ExpressionNode<int64_t>*
parse_const_expr(ErlNifEnv* env, const ERL_NIF_TERM& operand)
{
        int64_t val = eleveldb::ErlUtil::getValAsInt(env, operand);
        return new ConstantValue<int64_t>(val);
}

template<>
ExpressionNode<double>*
parse_const_expr(ErlNifEnv* env, const ERL_NIF_TERM& operand)
{
        double val = eleveldb::ErlUtil::getValAsDouble(env, operand);
        return new ConstantValue<double>(val);
}

template<>
ExpressionNode<std::string>*
parse_const_expr(ErlNifEnv* env, const ERL_NIF_TERM& operand)
{
        return new ConstantValue<std::string>(eleveldb::ErlUtil::getAsString(env, operand));
}


//=======================================================================
// Parse an expression
//=======================================================================

DataType::Type deduce_type(DataType::Type t1, DataType::Type t2)
{
        if (t1 == DataType::Type::NIL)
                return t2;
        else
                return t1;
}

template<typename T>
void*
parse_expression_node(ErlNifEnv* env, const ERL_NIF_TERM& root, const Extractor& ext)
{
        const std::vector<ERL_NIF_TERM> args = ErlUtil::getTupleCells(env, root);

        std::string op = ErlUtil::getAtom(env, args[0]);
        if (args.size() == 3 ) {
                auto expr_type = deduce_type(ext.cTypeOf(env, args[1]),
                                             ext.cTypeOf(env, args[2]));

                if (op == eleveldb::filter::EQ_OP ||
                    op == eleveldb::filter::EQEQ_OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<EqOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<EqOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<EqOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: return make_biary_op_of_type<EqOperator<bool>, bool>(args, env, ext);
                        default: ThrowRuntimeError("Can you even parse = " << expr_type);
                        }

                else if (op == eleveldb::filter::NEQ_OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<NeqOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<NeqOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<NeqOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: return make_biary_op_of_type<NeqOperator<bool>, bool>(args, env, ext);
                        default: ThrowRuntimeError("Can you even parse !=" << expr_type);
                        }

                else if (op == eleveldb::filter::LT_OP) {
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<LtOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<LtOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<LtOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: ThrowRuntimeError("Operation '<' not supported for type " << expr_type);
                        default: ThrowRuntimeError("Can you even parse?");
                        }

                } else if (op == eleveldb::filter::LTE_OP ||
                           op == eleveldb::filter::ELT_OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<LteOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<LteOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<LteOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: ThrowRuntimeError("Operation '=<' not supported for type " << expr_type);
                        default: ThrowRuntimeError("Can you even parse?");
                        }

                else if (op == eleveldb::filter::GT_OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<GtOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<GtOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<GtOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: ThrowRuntimeError("Operation '>' not supported for type " << expr_type);
                        default: ThrowRuntimeError("Can you even parse?");
                        }

                else if (op == eleveldb::filter::GTE_OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<GteOperator<int64_t>, int64_t>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<GteOperator<double>, double>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<GteOperator<std::string>, std::string>(args, env, ext);
                        case DataType::Type::BOOL: ThrowRuntimeError("Operation '>=' not supported for type " << expr_type);
                        default: ThrowRuntimeError("Can you even parse?");
                        }

                else if (op == eleveldb::filter::AND_OP ||
                         op == eleveldb::filter::AND__OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<AndOperator, bool>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<AndOperator, bool>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<AndOperator, bool>(args, env, ext);
                        case DataType::Type::BOOL: return make_biary_op_of_type<AndOperator, bool>(args, env, ext);
                        default: ThrowRuntimeError("Can you even parse?");
                        }

                else if (op == eleveldb::filter::OR_OP ||
                         op == eleveldb::filter::OR__OP)
                        switch (expr_type) {
                        case DataType::Type::INT: return make_biary_op_of_type<OrOperator, bool>(args, env, ext);
                        case DataType::Type::DOUBLE: return make_biary_op_of_type<OrOperator, bool>(args, env, ext);
                        case DataType::Type::STRING: return make_biary_op_of_type<OrOperator, bool>(args, env, ext);
                        case DataType::Type::BOOL: return make_biary_op_of_type<OrOperator, bool>(args, env, ext);
                        default: ThrowRuntimeError("Can you even parse?");
                        }
        } else {
                if (op == eleveldb::filter::CONST_OP) {
                        auto t = ErlUtil::typeOf(env, args[1]);
                        if (t == DataType::Type::NIL) {
                                return new ConstantValue<T>();
                        } else
                                return parse_const_expr<T>(env, args[1]);
                } else if (op == eleveldb::filter::FIELD_OP)
                        return parse_field_expr<T>(env, args[1]);
        }
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));
}


//=======================================================================
// Parse an expression that is the value of a field
//=======================================================================

template<typename T> ExpressionNode<T>*
parse_field_expr(ErlNifEnv* env, const ERL_NIF_TERM& operand)
{
        const std::string fn = eleveldb::ErlUtil::getBinaryAsString(env, operand);
        return new FieldValue<T>(fn);
}

}  // anon namespace


//=======================================================================
// Top-level call to parse range-filter
//=======================================================================

ExpressionNode<bool>*
eleveldb::parse_range_filter_opts(ErlNifEnv* env, const ERL_NIF_TERM& options, const Extractor& ext)
{
        return (ExpressionNode<bool>*)parse_expression_node<bool>(env, options, ext);
}
