package db

import (
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
		default:
			return nil, errors.New("bad binary op")
		}

		return out, nil
	}
	panic("unreachable")
}
