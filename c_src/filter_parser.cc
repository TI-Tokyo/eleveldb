#include "filter_parser.h"

#include "DataType.h"
#include "ErlUtil.h"

//------------------------------------------------------------
// Get a new binary operator of the requested type.  If either the
// left or right expressions don't evaulate to a valid expression,
// return NULL for the binary operator.  This will have the effect of
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

#define NEW_BIN(ClassName, operandType, dataType)                       \
        {                                                               \
                ExpressionNode<operandType>* leftExp  = parse_expression_node<operandType>(env, args[1], ext, throwIfInvalid); \
                ExpressionNode<operandType>* rightExp = parse_expression_node<operandType>(env, args[2], ext, throwIfInvalid); \
                                                                        \
                if (leftExp == 0 || rightExp == 0) {                    \
                        return NULL;                                    \
                } else {                                                \
                        ClassName<operandType>* ptr = new ClassName<operandType>(leftExp, rightExp, dataType); \
                        return ptr;                                     \
                }                                                       \
        }                                                               \

//------------------------------------------------------------
// Allocate a binary operator of the correct type for the operands.
// If either argument refers to a field we don't know about, return
// NULL.  This will cause any enclosing comparator (i.e., AND, OR) to
// ignore the condition
//------------------------------------------------------------

#define SWITCH_TYPE(ClassName) {                                      \
        switch (type) {                                               \
        case DataType::UINT:                                          \
            NEW_BIN(ClassName, uint64_t,       type);                 \
            break;                                                    \
        case DataType::INT:                                           \
            NEW_BIN(ClassName, int64_t,        type);                 \
            break;                                                     \
        case DataType::DOUBLE:                                          \
            NEW_BIN(ClassName, double,         type);                   \
            break;                                                      \
        case DataType::STRING:                                          \
            NEW_BIN(ClassName, std::string,    type);                   \
            break;                                                      \
        case DataType::BOOL:                                            \
            NEW_BIN(ClassName, bool,           type);                   \
            break;                                                      \
        default:                                                        \
            if(throwIfInvalid) {                                        \
                ThrowRuntimeError("Unsupported type: " << type);        \
            }                                                           \
            return NULL;                                                \
            break;                                                      \
        };                                                              \
    }

//=======================================================================
// Templates for expression parsing
//=======================================================================

template<typename T> ExpressionNode<T>*
parse_const_expr(ErlNifEnv*, ERL_NIF_TERM) {
        ThrowRuntimeError("Called an unsupported version of parse_const_expr");
        return NULL;
}

template<> ExpressionNode<bool>*
parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand)
{
        int64_t val = eleveldb::ErlUtil::getValAsBoolean(env, operand);
        return new ConstantValue<bool>(DataType::INT, val);
}

template<> ExpressionNode<int64_t>*
parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand)
{
        int64_t val = eleveldb::ErlUtil::getValAsInt(env, operand);
        return new ConstantValue<int64_t>(DataType::INT, val);
}

template<> ExpressionNode<uint64_t>*
parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand)
{
        uint64_t val = eleveldb::ErlUtil::getValAsUint(env, operand);
        return new ConstantValue<uint64_t>(DataType::UINT, val);
}

template<> ExpressionNode<double>*
parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand)
{
        double val = eleveldb::ErlUtil::getValAsDouble(env, operand);
        return new ConstantValue<double>(DataType::DOUBLE, val);
}

template<> ExpressionNode<std::string>*
parse_const_expr(ErlNifEnv* env, ERL_NIF_TERM operand)
{
        return new ConstantValue<std::string>(DataType::STRING, eleveldb::ErlUtil::getAsString(env, operand));
}

template<typename T> ExpressionNode<T>*
parse_expression_node(ErlNifEnv* env, ERL_NIF_TERM root, Extractor& ext, bool throwIfInvalid) {
        try {

                std::vector<ERL_NIF_TERM> opArgs = ErlUtil::getTupleCells(env, root);

                //------------------------------------------------------------
                // This tuple should be at least 2 items long, ie:
                //
                // {field, Name}, {field, Name, type} or
                // {const, Val},  {const, Val,  type}
                //
                //------------------------------------------------------------

                if (opArgs.size() >= 2) {

                        std::string op = ErlUtil::getAtom(env, opArgs[0]);

                        if (op == eleveldb::filter::CONST_OP) {
                                ExpressionNode<T>* ptr = parse_const_expr<T>(env, opArgs[1]);
                                return ptr;
                        }

                        if (op == eleveldb::filter::FIELD_OP) {
                                ExpressionNode<T>* ptr = parse_field_expr<T>(env, opArgs[1], ext);
                                return ptr;
                        }

                        if (throwIfInvalid)
                                ThrowRuntimeError("Unrecognized operator '" << op << "' while parsing: '"
                                                  << ErlUtil::formatTupleVec(env, opArgs) << "'");
                }

                if (throwIfInvalid)
                        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, opArgs));

        } catch (std::runtime_error& err) {
                if (throwIfInvalid)
                        throw err;
        }

        return nullptr;
}

//=======================================================================
// Parse an expression
//=======================================================================

ExpressionNode<bool>*
parse_root_expression_node(ErlNifEnv* env, ERL_NIF_TERM root, Extractor& ext, bool throwIfInvalid)
{
    try {
            std::vector<ERL_NIF_TERM> opArgs = ErlUtil::getTupleCells(env, root);

            if (opArgs.size() == 3) {

                    std::string op = ErlUtil::getString(env, opArgs[0]);

                    if (op == eleveldb::filter::EQ_OP ||
                        op == eleveldb::filter::EQEQ_OP)
                            return parse_equals_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::NEQ_OP)
                            return parse_nequals_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::LT_OP)
                            return parse_lt_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::LTE_OP ||
                             op == eleveldb::filter::ELT_OP)
                            return parse_lte_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::GT_OP)
                            return parse_gt_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::GTE_OP)
                            return parse_gte_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::AND_OP ||
                             op == eleveldb::filter::AND__OP)
                            return parse_and_expr(env, opArgs, ext, throwIfInvalid);
                    else if (op == eleveldb::filter::OR_OP ||
                             op == eleveldb::filter::OR__OP)
                            return parse_or_expr(env, opArgs, ext, throwIfInvalid);

                    if (throwIfInvalid)
                            ThrowRuntimeError("Unrecognized operator '" << op << "' while parsing: '"
                                              << ErlUtil::formatTupleVec(env, opArgs) << "'");
            }

            if (throwIfInvalid)
                    ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, opArgs));

    } catch (const std::runtime_error& err) {
            if (throwIfInvalid)
                    throw err;
    }

    return nullptr;
}

//=======================================================================
// Parse an == expression
//=======================================================================

ExpressionNode<bool>*
parse_equals_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);
        SWITCH_TYPE(EqOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse a != expression
//=======================================================================

ExpressionNode<bool>*
parse_nequals_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);
        SWITCH_TYPE(NeqOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse a < expression
//=======================================================================

ExpressionNode<bool>*
parse_lt_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);

        // Operation < not supported for booleans or binary types

        if (type == DataType::BOOL) {
            if(throwIfInvalid) {
                ThrowRuntimeError("Operation '" << ErlUtil::formatTerm(env, args[0]) << "' not supported for type: " << type);
            } else {
                return NULL;
            }
        }

        SWITCH_TYPE(LtOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse a <= expression
//=======================================================================

ExpressionNode<bool>*
parse_lte_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);

        // Operation <= not supported for booleans or binary types

        if (type == DataType::BOOL) {
            if(throwIfInvalid) {
                ThrowRuntimeError("Operation '" << ErlUtil::formatTerm(env, args[0]) << "' not supported for type: " << type);
            } else {
                return NULL;
            }
        }

        SWITCH_TYPE(LteOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse a > expression
//=======================================================================

ExpressionNode<bool>*
parse_gt_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);

        // Operation > not supported for booleans or binary types

        if (type == DataType::BOOL) {
            if(throwIfInvalid) {
                ThrowRuntimeError("Operation '" << ErlUtil::formatTerm(env, args[0]) << "' not supported for type: " << type);
            } else {
                return NULL;
            }
        }

        SWITCH_TYPE(GtOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse a >= expression
//=======================================================================

ExpressionNode<bool>*
parse_gte_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {
        DataType::Type type = ext.cTypeOf(env, args[1], args[2], throwIfInvalid);

        // Operation >= not supported for booleans or binary types

        if (type == DataType::BOOL) {
            if(throwIfInvalid) {
                ThrowRuntimeError("Operation '" << ErlUtil::formatTerm(env, args[0]) << "' not supported for type: " << type);
            } else {
                return NULL;
            }
        }

        SWITCH_TYPE(GteOperator);
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse an AND expression
//=======================================================================

ExpressionNode<bool>*
parse_and_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {

        ExpressionNode<bool>* left  = parse_expression_node<bool>(env, args[1], ext, throwIfInvalid);
        ExpressionNode<bool>* right = parse_expression_node<bool>(env, args[2], ext, throwIfInvalid);

        // If both expressions are non-NULL, return the
        // AndOperator

        if(left && right) {
            return new AndOperator(left, right);

            // Else replace the AND operation with whichever
            // condition is non-NULL.  This is tantamount to
            // simply not evaluating a condition that refers to a
            // field we don't know about.
            //
            // NB: we don't throw here, even if throwIfInvalid is true,
            // since in that case parse_expression_node will already
            // have thrown rather than return a NULL

        } else {

            if(left)
                return left;
            if(right)
                return right;
        }
    }

    if(throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse an OR expression
//=======================================================================

ExpressionNode<bool>*
parse_or_expr(ErlNifEnv* env, std::vector<ERL_NIF_TERM>& args, Extractor& ext, bool throwIfInvalid) {
    if(args.size() == 3) {

        ExpressionNode<bool>* left  = parse_expression_node<bool>(env, args[1], ext, throwIfInvalid);
        ExpressionNode<bool>* right = parse_expression_node<bool>(env, args[2], ext, throwIfInvalid);

        // If both expressions are non-NULL, return the
        // OrOperator as requested

        if (left && right) {
            return new OrOperator(left, right);

            // Else replace the OR operation with whichever
            // condition is non-NULL.  This is tantamount to simply
            // not evaluating a condition that refers to a field
            // we don't know about.
            //
            // NB: we don't throw here, even if throwIfInvalid is true,
            // since in that case parse_expression_node will already
            // have thrown rather than return a NULL

        } else {
            if (left)
                return left;
            if (right)
                return right;
        }
    }

    if (throwIfInvalid)
        ThrowRuntimeError("Malformed tuple: " << ErlUtil::formatTupleVec(env, args));

    return NULL;
}

//=======================================================================
// Parse an expression that is the value of a field
//=======================================================================

template<typename T> ExpressionNode<T>*
parse_field_expr(ErlNifEnv* env, ERL_NIF_TERM operand, Extractor& ext)
{
        std::string fieldName = eleveldb::ErlUtil::getBinaryAsString(env, operand);

        // Each field must be added to all extractors that might be used to
        // filter data

        ext.add_field(fieldName);

        return new FieldValue<T>(fieldName, ext.cTypeOf(fieldName));
}

//=======================================================================
// Top-level call to parse range-filter
//=======================================================================

ExpressionNode<bool>*
parse_range_filter_opts(ErlNifEnv* env, ERL_NIF_TERM options, Extractor& ext, bool throwIfInvalid)
{
    return parse_root_expression_node(env, options, ext, throwIfInvalid);
}
