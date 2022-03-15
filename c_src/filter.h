#ifndef operator_hpp
#define operator_hpp

#include <iostream>
#include <vector>

#include <cstring>

#include "exceptionutils.h"
#include "DataType.h"

#include <fstream>
#include <ios>

using namespace eleveldb;

//=======================================================================
// A template base-class for all expression nodes
//=======================================================================

template<typename TResult>
struct ExpressionNode {
public:
        ExpressionNode(DataType::Type type) :
                type_(type)
                {}

        virtual ~ExpressionNode() {}

        virtual TResult evaluate() const = 0;

        virtual void set_value(const std::string& key, void* val,
                               DataType::Type type) = 0;

        virtual void checkType(DataType::Type type) const
                {
                        // If the type is unknown, we can't check it

                        if (type_ == DataType::UNKNOWN)
                                ThrowRuntimeError("Unable to check the type of this expression");

                        if (type_ != type)
                                ThrowRuntimeError(
                                        "Attempt to set the wrong type of value (" << type
                                        << ") for this expression, which is of type " << type_);
                }

        DataType::Type type_;
};

//=======================================================================
// Base-class for expressions involving two operators (binary operators)
//=======================================================================

template<typename TResult, typename TOperands>
class BinaryExpression : public ExpressionNode<TResult> {
protected:
        ExpressionNode<TOperands>* left_;
        ExpressionNode<TOperands>* right_;

public:
        BinaryExpression(ExpressionNode<TOperands>* left,
                         ExpressionNode<TOperands>* right,
                         DataType::Type type) :
                ExpressionNode<TResult>(type),
                left_(left),
                right_(right)
                {}

        virtual ~BinaryExpression() {
                if (left_)
                        delete left_;
                if (right_)
                        delete right_;
        }

        void set_value(const std::string& key, void* value,
                       DataType::Type type)
                {
                        left_->set_value(key, value, type);
                        right_->set_value(key, value, type);
                }
};

//=======================================================================
// Specializations of binary operators
//=======================================================================

//------------------------------------------------------------
// AND
//------------------------------------------------------------

class AndOperator: public BinaryExpression<bool, bool> {
public:
        AndOperator(ExpressionNode<bool>* left, ExpressionNode<bool>* right) :
                BinaryExpression<bool, bool>(left, right, DataType::BOOL)
                {}

        bool evaluate() const
                {
                        // Note: now that we are allowing NULLs, we can no longer
                        // check binary has_value() in AndOperator.
                        //
                        // What if one of our conditions is a NULL comparison, e.g.,
                        // 'f1 == []'?  Then left->has_value()==false should _not_
                        // cause us to return false here!
                        //
                        // We must instead evaluate both clauses, and allow the
                        // evaluate functions to determine whether or not
                        // has_value()==false is an error condition

                        return left_->evaluate() && right_->evaluate();
                }
};

//------------------------------------------------------------
// OR
//------------------------------------------------------------

class OrOperator: public BinaryExpression<bool, bool> {
public:
        OrOperator(ExpressionNode<bool>* left, ExpressionNode<bool>* right) :
                BinaryExpression<bool, bool>(left, right, DataType::BOOL)
                {}

        bool evaluate() const
                {
                        return left_->evaluate() || right_->evaluate();
                }
};

//------------------------------------------------------------
// > operator
//------------------------------------------------------------

template<typename T>
class GtOperator: public BinaryExpression<bool, T> {
public:
        GtOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right,
                   DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() > this->right_->evaluate();
                }
};

//------------------------------------------------------------
// >= operator
//------------------------------------------------------------

template<typename T>
class GteOperator : public BinaryExpression<bool, T> {
public:
        GteOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right,
                    DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() >= this->right_->evaluate();
                }
};

//------------------------------------------------------------
// < operator
//------------------------------------------------------------

template<typename T>
class LtOperator : public BinaryExpression<bool, T> {
public:
        LtOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right,
                   DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() < this->right_->evaluate();
                }
};

//------------------------------------------------------------
// <= operator
//------------------------------------------------------------

template<typename T>
class LteOperator : public BinaryExpression<bool, T> {
public:
        LteOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right,
                    DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() <= this->right_->evaluate();
                }
};

//------------------------------------------------------------
// == operator
//------------------------------------------------------------

template<typename T>
class EqOperator: public BinaryExpression<bool, T> {
public:
        EqOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right,
                   DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() == this->right_->evaluate();
                }
};

//------------------------------------------------------------
// != operator
//------------------------------------------------------------

template<typename T>
class NeqOperator: public BinaryExpression<bool, T> {
public:
        NeqOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right,
                    DataType::Type type) :
                BinaryExpression<bool, T>(left, right, type)
                {}

        bool evaluate() const
                {
                        return this->left_->evaluate() != this->right_->evaluate();
                }
};


//=======================================================================
// Unary operators
//=======================================================================

//------------------------------------------------------------
// Constant value
//------------------------------------------------------------

template<typename T>
struct ConstantValue : public ExpressionNode<T> {
    const T value_;

        ConstantValue(DataType::Type type, T val) :
            ExpressionNode<T>(type),
            value_(val)
                {}

        T evaluate() const
                {
                        return value_;
                }

        virtual void set_value(const std::string& key, void* val, DataType::Type)
                {
                        // noop for constant
                }
};

//------------------------------------------------------------
// Field value
//------------------------------------------------------------

template<typename T>
struct FieldValue : public ExpressionNode<T> {
        const std::string field_;
        T value_;

        FieldValue(const std::string fieldName, DataType::Type type) :
                ExpressionNode<T>(type),
                field_(fieldName)
                {}

        T evaluate() const
                {
                        return value_;
                }

        void checkType(const DataType::Type type) const {
                try {
                        ExpressionNode<T>::checkType(type);
                } catch (std::runtime_error& err) {
                        ThrowRuntimeError(err.what() << ", while processing field " << field_);
                }
        }

        virtual void set_value(const std::string& key, void* val,
                               DataType::Type type) {

                // If called from a BinaryOperator parent, only set/check the
                // value for the matching field

                if (key == field_) {
                        value_ = *((T*)val);
                        ExpressionNode<T>::type_ = type;
                }
        }
};


#endif
