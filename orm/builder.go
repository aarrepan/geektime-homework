package orm

import (
	"gitee.com/geektime-geekbang/geektime-go/orm/internal/errs"
	"strings"
)

type builder struct {
	core
	sb strings.Builder
	args []any
	quoter byte
}

func (b *builder) quote(name string) {
	b.sb.WriteByte(b.quoter)
	b.sb.WriteString(name)
	b.sb.WriteByte(b.quoter)
}


func (b *builder) buildColumn(c Column) error {
	switch table := c.table.(type) {
	case nil:
		fd, ok := b.model.FieldMap[c.name]
		// 字段不对，或者说列不对
		if !ok {
			return errs.NewErrUnknownField(c.name)
		}
		b.quote(fd.ColName)
		if c.alias != "" {
			b.sb.WriteString(" AS ")
			b.quote(c.alias)
		}
	case Table:
		m, err := b.r.Get(table.entity)
		if err != nil {
			return err
		}
		fd, ok := m.FieldMap[c.name]
		if !ok {
			return errs.NewErrUnknownField(c.name)
		}
		if table.alias != "" {
			b.quote(table.alias)
			b.sb.WriteByte('.')
		}
		b.quote(fd.ColName)
		if c.alias != "" {
			b.sb.WriteString(" AS ")
			b.quote(c.alias)
		}
	default:
		return errs.NewErrUnsupportedTable(table)
	}
	return nil
}

func (b *builder) addArg(vals ...any) {
	if len(vals) == 0 {
		return
	}
	if b.args == nil {
		b.args = make([]any, 0, 8)
	}
	b.args = append(b.args, vals...)
}