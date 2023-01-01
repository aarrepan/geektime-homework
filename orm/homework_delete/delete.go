package homework_delete

import (
	"reflect"
)

type Deleter[T any] struct {
	builder
	table string
	where []Predicate
}

func (d *Deleter[T]) Build() (*Query, error) {
	_, _ = d.sqlCmd.WriteString("DELETE FROM ")
	//默认使用model名字作为表明
	if d.table == "" {
		var t T
		d.sqlCmd.WriteByte('`')
		d.sqlCmd.WriteString(reflect.TypeOf(t).Name())
		d.sqlCmd.WriteByte('`')
	} else {
		d.sqlCmd.WriteString(d.table)
	}
	if len(d.where) > 0 {
		d.sqlCmd.WriteString(" WHERE ")
		err := d.buildPredicates(d.where)
		if err != nil {
			return nil, err
		}
	}
	d.sqlCmd.WriteByte(';')
	return &Query{SQL: d.sqlCmd.String(), Args: d.args}, nil
}

// From accepts model definition
func (d *Deleter[T]) From(table string) *Deleter[T] {
	d.table = table
	return d
}

// Where accepts predicates
func (d *Deleter[T]) Where(predicates ...Predicate) *Deleter[T] {
	d.where = predicates
	return d
}
