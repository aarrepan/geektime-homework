package homework_delete

import (
	"fmt"
	"strings"
)

type builder struct {
	sqlCmd strings.Builder
	args   []any
}

func (b *builder) buildPredicates(predList []Predicate) error {
	p := predList[0]
	for i := 1; i < len(predList); i++ {
		p = p.And(predList[i])
	}
	return b.buildExpression(p)
}

func (b *builder) buildExpression(e Expression) error {
	if e == nil {
		return nil
	}
	switch exp := e.(type) {
	case Column:
		b.sqlCmd.WriteByte('`')
		b.sqlCmd.WriteString(exp.name)
		b.sqlCmd.WriteByte('`')
	case value:
		b.sqlCmd.WriteByte('?')
		b.args = append(b.args, exp.val)
	case Predicate:
		_, lp := exp.left.(Predicate)
		if lp {
			b.sqlCmd.WriteByte('(')
		}
		if err := b.buildExpression(exp.left); err != nil {
			return err
		}
		if lp {
			b.sqlCmd.WriteByte(')')
		}

		b.sqlCmd.WriteString(exp.op.String())

		_, rp := exp.right.(Predicate)
		if rp {
			b.sqlCmd.WriteByte('(')
		}

		err := b.buildExpression(exp.right)
		if err != nil {
			return err
		}
		if rp {
			b.sqlCmd.WriteByte(')')
		}
	default:
		return fmt.Errorf("orm: 不支持的表达式 %v", exp)
	}
	return nil
}
