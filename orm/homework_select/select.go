package orm

import (
	"context"
	"database/sql"
	"strings"

	"gitee.com/geektime-geekbang/geektime-go/orm/homework_select/internal/errs"
	"gitee.com/geektime-geekbang/geektime-go/orm/homework_select/model"
)

// Selector 用于构造 SELECT 语句
type Selector[T any] struct {
	db      *DB
	sb      strings.Builder
	alias   []string
	args    []any
	table   string
	where   []Predicate
	having  []Predicate
	model   *model.Model
	columns []Selectable
	groupBy []Column
	orderBy []OrderBy
	offset  int
	limit   int
}

// Select 执行查询列名列表，如果是空，代表使用 select * 查询
func (s *Selector[T]) Select(cols ...Selectable) *Selector[T] {
	s.columns = cols
	return s
}

// From 指定表名，如果是空字符串，那么将会使用默认表名
func (s *Selector[T]) From(tbl string) *Selector[T] {
	s.table = tbl
	return s
}

func (s *Selector[T]) Build() (*Query, error) {
	var (
		t   T
		err error
	)
	s.model, err = s.db.r.Get(&t)
	if err != nil {
		return nil, err
	}
	s.sb.WriteString("SELECT ")
	if err = s.buildColumns(); err != nil {
		return nil, err
	}

	s.sb.WriteString(" FROM ")
	if s.table == "" {
		s.sb.WriteByte('`')
		s.sb.WriteString(s.model.TableName)
		s.sb.WriteByte('`')
	} else {
		s.sb.WriteString(s.table)
	}

	// 构造 WHERE
	if len(s.where) > 0 {
		s.sb.WriteString(" WHERE ")
		// WHERE 不允许用别名
		if err = s.buildPredicates(s.where, false); err != nil {
			return nil, err
		}
	}

	if len(s.groupBy) > 0 {
		s.sb.WriteString(" GROUP BY ")
		for i, c := range s.groupBy {
			if i > 0 {
				s.sb.WriteByte(',')
			}
			if err = s.buildColumn(c.name, c.alias); err != nil {
				return nil, err
			}
		}
	}

	if len(s.having) > 0 {
		s.sb.WriteString(" HAVING ")
		if err = s.buildPredicates(s.having, true); err != nil {
			return nil, err
		}
	}

	if len(s.orderBy) > 0 {
		s.sb.WriteString(" ORDER BY ")
		if err = s.buildOrderBy(); err != nil {
			return nil, err
		}
	}

	if s.limit > 0 {
		s.sb.WriteString(" LIMIT ?")
		s.addArgs(s.limit)
	}

	if s.offset > 0 {
		s.sb.WriteString(" OFFSET ?")
		s.addArgs(s.offset)
	}

	s.sb.WriteString(";")
	return &Query{
		SQL:  s.sb.String(),
		Args: s.args,
	}, nil
}

func (s *Selector[T]) buildOrderBy() error {
	for idx, ob := range s.orderBy {
		if idx > 0 {
			s.sb.WriteByte(',')
		}
		err := s.buildColumn(ob.col, "")
		if err != nil {
			return err
		}
		s.sb.WriteByte(' ')
		s.sb.WriteString(ob.order)
	}
	return nil
}

func (s *Selector[T]) buildPredicates(ps []Predicate, useAlias bool) error {
	predExpress := ps[0]
	for idx := 1; idx < len(ps); idx++ {
		predExpress = predExpress.And(ps[idx])
	}
	return s.buildExpression(predExpress, useAlias)
}

func (s *Selector[T]) buildColumns() error {
	if len(s.columns) == 0 {
		s.sb.WriteByte('*')
		return nil
	}
	for idx, c := range s.columns {
		if idx > 0 {
			s.sb.WriteByte(',')
		}
		switch val := c.(type) {
		case Column:
			if err := s.buildColumn(val.name, val.alias); err != nil {
				return err
			}
		case Aggregate:
			if err := s.buildAggregate(val, true); err != nil {
				return err
			}
		case RawExpr:
			s.sb.WriteString(val.raw)
			if len(val.args) != 0 {
				s.addArgs(val.args...)
			}
		default:
			return errs.NewErrUnsupportedSelectable(c)
		}
	}
	return nil
}

func (s *Selector[T]) buildAggregate(a Aggregate, useAlias bool) error {
	s.sb.WriteString(a.fn)
	s.sb.WriteString("(`")

	fieldName, ok := s.model.FieldMap[a.arg]
	if !ok {
		return errs.NewErrUnknownField(a.arg)
	}
	s.sb.WriteString(fieldName.ColName)
	s.sb.WriteString("`)")
	if useAlias {
		s.buildAs(a.alias)
	}
	return nil
}

func (s *Selector[T]) buildColumn(c string, alias string) error {
	s.sb.WriteByte('`')
	fieldName, ok := s.model.FieldMap[c]
	if !ok {
		return errs.NewErrUnknownField(c)
	}
	s.sb.WriteString(fieldName.ColName)
	s.sb.WriteByte('`')
	if alias != "" {
		s.buildAs(alias)
	}
	return nil
}

func (s *Selector[T]) buildExpression(e Expression, useAlias bool) error {
	if e == nil {
		return nil
	}
	switch exp := e.(type) {
	case Column:
		if !useAlias {
			return s.buildColumn(exp.name, "")
		}
		return s.buildColumn(exp.name, exp.alias)
	case Aggregate:
		return s.buildAggregate(exp, false)
	case value:
		s.sb.WriteByte('?')
		s.addArgs(exp.val)
	case RawExpr:
		s.sb.WriteString(exp.raw)
		if len(exp.args) != 0 {
			s.addArgs(exp.args...)
		}
	case Predicate:
		_, leftPred := exp.left.(Predicate)
		if leftPred {
			s.sb.WriteByte('(')
		}
		if err := s.buildExpression(exp.left, useAlias); err != nil {
			return err
		}
		if leftPred {
			s.sb.WriteByte(')')
		}

		// 可能只有左边
		if exp.op == "" {
			return nil
		}

		s.sb.WriteByte(' ')
		s.sb.WriteString(exp.op.String())
		s.sb.WriteByte(' ')

		_, rightPred := exp.right.(Predicate)
		if rightPred {
			s.sb.WriteByte('(')
		}
		if err := s.buildExpression(exp.right, useAlias); err != nil {
			return err
		}
		if rightPred {
			s.sb.WriteByte(')')
		}
	default:
		return errs.NewErrUnsupportedExpressionType(exp)
	}
	return nil
}

// Where 用于构造 WHERE 查询条件。如果 ps 长度为 0，那么不会构造 WHERE 部分
func (s *Selector[T]) Where(ps ...Predicate) *Selector[T] {
	s.where = ps
	return s
}

// GroupBy 设置 group by 子句
func (s *Selector[T]) GroupBy(cols ...Column) *Selector[T] {
	s.groupBy = cols
	return s
}

func (s *Selector[T]) Having(ps ...Predicate) *Selector[T] {
	s.having = ps
	return s
}

func (s *Selector[T]) Offset(offset int) *Selector[T] {
	s.offset = offset
	return s
}

func (s *Selector[T]) Limit(limit int) *Selector[T] {
	s.limit = limit
	return s
}

func (s *Selector[T]) OrderBy(orderBys ...OrderBy) *Selector[T] {
	s.orderBy = orderBys
	return s
}

func (s *Selector[T]) Get(ctx context.Context) (*T, error) {
	getQuery, err := s.Build()
	if err != nil {
		return nil, err
	}
	// s.db 是我们定义的 DB
	// s.db.db 则是 sql.DB
	// 使用 QueryContext，从而和 GetMulti 能够复用处理结果集的代码
	rows, err := s.db.db.QueryContext(ctx, getQuery.SQL, getQuery.Args...)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, ErrNoRows
	}

	tp := new(T)
	meta, err := s.db.r.Get(tp)
	if err != nil {
		return nil, err
	}
	val := s.db.valCreator(tp, meta)
	err = val.SetColumns(rows)
	return tp, err
}

func (s *Selector[T]) addArgs(args ...any) {
	if s.args == nil {
		s.args = make([]any, 0, 8)
	}
	s.args = append(s.args, args...)
}

func (s *Selector[T]) buildAs(alias string) {
	if alias != "" {
		s.sb.WriteString(" AS `")
		s.sb.WriteString(alias)
		s.sb.WriteByte('`')
	}
}

func (s *Selector[T]) GetMulti(ctx context.Context) ([]*T, error) {
	var db sql.DB
	getQuery, err := s.Build()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, getQuery.SQL, getQuery.Args...)
	if err != nil {
		return nil, err
	}

	rtnSelect := make([]*T, 0)
	_, err = rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		// 在这里构造 []*T
		tp := new(T)
		meta, err := s.db.r.Get(tp)
		if err != nil {
			return nil, err
		}
		val := s.db.valCreator(tp, meta)
		err = val.SetColumns(rows)
		rtnSelect = append(rtnSelect, tp)
	}
	return rtnSelect, nil
}

func NewSelector[T any](db *DB) *Selector[T] {
	return &Selector[T]{
		db: db,
	}
}

type Selectable interface {
	selectable()
}

type OrderBy struct {
	col   string
	order string
}

func Asc(col string) OrderBy {
	return OrderBy{
		col:   col,
		order: "ASC",
	}
}

func Desc(col string) OrderBy {
	return OrderBy{
		col:   col,
		order: "DESC",
	}
}
