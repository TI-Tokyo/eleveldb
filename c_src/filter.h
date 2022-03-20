#ifndef FILTER_H
#define FILTER_H

#include <typeinfo>
#include <vector>

#include <cstring>

#include "exceptionutils.h"
#include "DataType.h"

using namespace eleveldb;

static bool NonNullTrue = true;
static bool NonNullFalse = false;

template<typename T>
struct NullableValue {
        const T* v;
        NullableValue(const T* value) : v(value) {}
        NullableValue(bool a) : v((const T*)(a ? &NonNullTrue : &NonNullFalse)) {}

        bool is_true() { return v and *v; }

        bool operator==(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v == *rhs.v;
                        else
                                return v == nullptr and rhs.v == nullptr;
                }
        bool operator!=(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v != *rhs.v;
                        else
                                return (v and !rhs.v) or (!v and rhs.v);
                }
        bool operator>(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v > *rhs.v;
                        else if (!v and !rhs.v)
                                return false;
                        else
                                return !!v;
                }
        bool operator<(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v < *rhs.v;
                        else if (!v and !rhs.v)
                                return false;
                        else
                                return !!rhs.v;
                }
        bool operator>=(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v >= *rhs.v;
                        else if (!v and !rhs.v)
                                return true;
                        else
                                return !!v;
                }
        bool operator<=(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v <= *rhs.v;
                        else if (!v and !rhs.v)
                                return true;
                        else
                                return !!rhs.v;
                }
        bool operator&&(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v && *rhs.v;
                        else
                                return false;
                }
        bool operator||(const NullableValue<T>& rhs) const
                {
                        if (v and rhs.v)
                                return *v || *rhs.v;
                        else
                                return (v and *v) or (rhs.v and *rhs.v);
                }
};


//=======================================================================
// A template base-class for all expression nodes
//=======================================================================

template<typename T>
struct ExpressionNode {
public:
        virtual ~ExpressionNode() {}
        virtual NullableValue<T> evaluate() const;
        virtual void set_value(const std::string& key, void* val, DataType::Type) const;
};

//=======================================================================
// Base-class for expressions involving two operators (binary operators)
//=======================================================================

template<typename T>
class BinaryExpression : public ExpressionNode<T> {
protected:
        ExpressionNode<T>* l;
        ExpressionNode<T>* r;

public:
        BinaryExpression(ExpressionNode<T>* left,
                         ExpressionNode<T>* right)
                : ExpressionNode<T>(),
                  l(left),
                  r(right)
                {}

        virtual ~BinaryExpression()
                {
                        delete l;
                        delete r;
                }
        virtual NullableValue<T> evaluate() const = 0;

        void set_value(const std::string& k, void* v, DataType::Type t) const;
};

//=======================================================================
// Specializations of binary operators
//=======================================================================

class AndOperator: public BinaryExpression<bool> {
public:
        AndOperator(ExpressionNode<bool>* left,
                    ExpressionNode<bool>* right)
                : BinaryExpression<bool>(left, right)
                {}

        NullableValue<bool> evaluate() const { return NullableValue<bool>(this->l->evaluate() && this->r->evaluate()); }
};

class OrOperator: public BinaryExpression<bool> {
public:
        OrOperator(ExpressionNode<bool>* left,
                   ExpressionNode<bool>* right)
                : BinaryExpression<bool>(left, right)
                {}

        NullableValue<bool> evaluate() const { return NullableValue<bool>(this->l->evaluate() || this->r->evaluate()); }
};

template<typename T>
class GtOperator: public BinaryExpression<T> {
public:
        GtOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() > this->r->evaluate()); }
};

template<typename T>
class GteOperator : public BinaryExpression<T> {
public:
        GteOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() >= this->r->evaluate()); }
};

template<typename T>
class LtOperator : public BinaryExpression<T> {
public:
        LtOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() < this->r->evaluate()); }
};

template<typename T>
class LteOperator : public BinaryExpression<T> {
public:
        LteOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() <= this->r->evaluate()); }
};

template<typename T>
class EqOperator: public BinaryExpression<T> {
public:
        EqOperator(ExpressionNode<T>* left,
                   ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() == this->r->evaluate()); }
};

template<typename T>
class NeqOperator: public BinaryExpression<T> {
public:
        NeqOperator(ExpressionNode<T>* left,
                    ExpressionNode<T>* right)
                : BinaryExpression<T>(left, right)
                {}

        NullableValue<T> evaluate() const { return NullableValue<T>(this->l->evaluate() != this->r->evaluate()); }
};


//=======================================================================
// Unary operators
//=======================================================================

//------------------------------------------------------------
// Constant value
//------------------------------------------------------------


template<typename T>
class ConstantValue : public ExpressionNode<T> {
        const T* v;

public:
        ConstantValue(const T& val)
                : ExpressionNode<T>(),
                  v(new T(val))
                {}
        ConstantValue()
                : ExpressionNode<T>(),
                  v(nullptr)
                {}
        virtual ~ConstantValue()
                {
                        if (v)
                                delete v;
                }

        NullableValue<T> evaluate() const
                {
                        return NullableValue<T>(v);
                }

        void set_value(const std::string&, void*, DataType::Type)
                {
                        // noop for constant
                }
};

//------------------------------------------------------------
// Field value
//------------------------------------------------------------

template<typename T>
struct FieldValue : public ExpressionNode<T> {
        const std::string f;
        const T* v;

        FieldValue(const std::string& field)
                : ExpressionNode<T>(),
                  f(field),
                  v(nullptr)
                {}
        virtual ~FieldValue()
                {
                        if (v)
                                delete v;
                }

        NullableValue<T> evaluate() const
                {
                        return NullableValue<T>(v);
                }

        void set_value(const std::string& key, void* val, DataType::Type type)
                {
                        if (key == f) {
                                if (v)
                                        delete v;
                                if (type == DataType::Type::NIL)
                                        v = nullptr;
                                else
                                        v = new T(*(T*)val);
                        }
                }
};

#define TRY_AS(C) \
        {                                                               \
                auto us = dynamic_cast<const FieldValue<T>*>(this);     \
                if (us) {                                               \
                        return us->evaluate();                          \
                }                                                       \
        }


template <typename T>
NullableValue<T> ExpressionNode<T>::evaluate() const
{
        TRY_AS(FieldValue);
        TRY_AS(ConstantValue);
        TRY_AS(BinaryExpression);
        return {false};
}


template <typename T>
void ExpressionNode<T>::set_value(const std::string& k, void* v, DataType::Type t) const
{
        auto us = dynamic_cast<const BinaryExpression<T>*>(this);
        if (us)
                us->set_value(k, v, t);
}


template <typename T>
void BinaryExpression<T>::set_value(const std::string& k, void* v, DataType::Type t) const
{
        auto lreal = dynamic_cast<FieldValue<T>*>(l);
        if (lreal)
                lreal->set_value(k, v, t);
        else
                l->set_value(k, v, t);
        auto rreal = dynamic_cast<FieldValue<T>*>(r);
        if (rreal)
                rreal->set_value(k, v, t);
        else
                r->set_value(k, v, t);
}


#endif
