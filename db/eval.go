package db

import (
	"bytes"
	"cmp"
	"errors"
	"slices"
)

/* Evaluate a parsed SQL expression. */
func evalExpr(schema *Schema, row Row, expr interface{}) (*Cell, error) {
	switch e := expr.(type) {
	case string:
		// e is column name
		idx := slices.IndexFunc(schema.Cols, func(col Column) bool {
			return col.Name == e
		})
		if idx < 0 {
			return nil, errors.New("unknown column")
		}
		return &row[idx], nil
	case *Cell:
		// e is constant value
		return e, nil
	case *ExprUnOp:
		kid, err := evalExpr(schema, row, e.kid)
		if err != nil {
			return nil, err
		}

		if e.op == OP_NEG && kid.Type == TypeI64 {
			return &Cell{Type: TypeI64, I64: -kid.I64}, nil
		} else if e.op == OP_NOT && kid.Type == TypeI64 {
			b := int64(0)
			if kid.I64 == 0 {
				b = 1
			}
			return &Cell{Type: TypeI64, I64: b}, nil
		} else {
			return nil, errors.New("bad unary op")
		}
	case *ExprBinOp:
		left, err := evalExpr(schema, row, e.left)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(schema, row, e.right)
		if err != nil {
			return nil, err
		}

		if left.Type != right.Type {
			return nil, errors.New("binary op type mismatch")
		}
		out := &Cell{Type: left.Type}

		// comparison
		switch e.op {
		case OP_EQ, OP_NE, OP_LE, OP_GE, OP_LT, OP_GT:
			r := 0
			switch out.Type {
			case TypeI64:
				r = cmp.Compare(left.I64, right.I64)
			case TypeStr:
				r = bytes.Compare(left.Str, right.Str)
			default:
				panic("unreachable")
			}

			b := false
			switch e.op {
			case OP_EQ:
				b = (r == 0)
			case OP_NE:
				b = (r != 0)
			case OP_LE:
				b = (r <= 0)
			case OP_GE:
				b = (r >= 0)
			case OP_LT:
				b = (r < 0)
			case OP_GT:
				b = (r > 0)
			}
			if b {
				out.I64 = 1
			}
			return out, nil
		}

		switch {
		// string concatenation
		case out.Type == TypeStr && e.op == OP_ADD:
			out.Str = slices.Concat(left.Str, right.Str)
		// arithmetic
		case out.Type == TypeI64 && e.op == OP_ADD:
			out.I64 = left.I64 + right.I64
		case out.Type == TypeI64 && e.op == OP_SUB:
			out.I64 = left.I64 - right.I64
		case out.Type == TypeI64 && e.op == OP_MUL:
			out.I64 = left.I64 * right.I64
		case out.Type == TypeI64 && e.op == OP_DIV:
			if right.I64 == 0 {
				return nil, errors.New("division by 0")
			}
			out.I64 = left.I64 / right.I64
		// logic
		case out.Type == TypeI64 && e.op == OP_AND:
			if left.I64 != 0 && right.I64 != 0 {
				out.I64 = 1
			}
		case out.Type == TypeI64 && e.op == OP_OR:
			if left.I64 != 0 || right.I64 != 0 {
				out.I64 = 1
			}
		default:
			return nil, errors.New("bad binary op")
		}

		return out, nil
	}
	panic("unreachable")
}
