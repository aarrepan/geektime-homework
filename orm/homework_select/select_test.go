//go:build howework
package orm

import (
	"gitee.com/geektime-geekbang/geektime-go/orm/homework_select/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelector_OrderBy(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			name: "column",
			q:    NewSelector[TestModel](db).OrderBy(Asc("Age")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` ORDER BY `age` ASC;",
			},
		},
		{
			name: "columns",
			q:    NewSelector[TestModel](db).OrderBy(Asc("Age"), Desc("Id")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` ORDER BY `age` ASC,`id` DESC;",
			},
		},
		{
			name: "invalid column",
			q:    NewSelector[TestModel](db).OrderBy(Asc("Invalid")),
			wantErr: errs.NewErrUnknownField("Invalid"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestSelector_OffsetLimit(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			name: "offset only",
			q:    NewSelector[TestModel](db).Offset(10),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` OFFSET ?;",
				Args: []any{10},
			},
		},
		{
			name: "limit only",
			q:    NewSelector[TestModel](db).Limit(10),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` LIMIT ?;",
				Args: []any{10},
			},
		},
		{
			name: "limit offset",
			q:    NewSelector[TestModel](db).Limit(20).Offset(10),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` LIMIT ? OFFSET ?;",
				Args: []any{20, 10},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestSelector_Having(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			// ??????????????????????????????
			name: "none",
			q:    NewSelector[TestModel](db).GroupBy(C("Age")).Having(),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age`;",
			},
		},
		{
			// ????????????
			name: "single",
			q:    NewSelector[TestModel](db).GroupBy(C("Age")).
				Having(C("FirstName").EQ("Deng")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age` HAVING `first_name` = ?;",
				Args: []any{"Deng"},
			},
		},
		{
			// ????????????
			name: "multiple",
			q:    NewSelector[TestModel](db).GroupBy(C("Age")).
				Having(C("FirstName").EQ("Deng"), C("LastName").EQ("Ming")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age` HAVING (`first_name` = ?) AND (`last_name` = ?);",
				Args: []any{"Deng", "Ming"},
			},
		},
		{
			// ????????????
			name: "avg",
			q:    NewSelector[TestModel](db).GroupBy(C("Age")).
				Having(Avg("Age").EQ(18)),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age` HAVING AVG(`age`) = ?;",
				Args: []any{18},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestSelector_GroupBy(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			// ??????????????????????????????
			name: "none",
			q:    NewSelector[TestModel](db).GroupBy(),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model`;",
			},
		},
		{
			// ??????
			name: "single",
			q:    NewSelector[TestModel](db).GroupBy(C("Age")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age`;",
			},
		},
		{
			// ??????
			name: "multiple",
			q:    NewSelector[TestModel](db).GroupBy(C("Age"), C("FirstName")),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` GROUP BY `age`,`first_name`;",
			},
		},
		{
			// ?????????
			name: "invalid column",
			q:    NewSelector[TestModel](db).GroupBy(C("Invalid")),
			wantErr: errs.NewErrUnknownField("Invalid"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestSelector_Select(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			// ????????????
			name: "all",
			q:    NewSelector[TestModel](db),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model`;",
			},
		},
		{
			name: "invalid column",
			q:    NewSelector[TestModel](db).Select(Avg("Invalid")),
			wantErr: errs.NewErrUnknownField("Invalid"),
		},
		{
			name: "partial columns",
			q:    NewSelector[TestModel](db).Select(C("Id"), C("FirstName")),
			wantQuery: &Query{
				SQL: "SELECT `id`,`first_name` FROM `test_model`;",
			},
		},
		{
			name: "avg",
			q:    NewSelector[TestModel](db).Select(Avg("Age")),
			wantQuery: &Query{
				SQL: "SELECT AVG(`age`) FROM `test_model`;",
			},
		},
		{
			name: "raw expression",
			q:    NewSelector[TestModel](db).Select(Raw("COUNT(DISTINCT `first_name`)")),
			wantQuery: &Query{
				SQL: "SELECT COUNT(DISTINCT `first_name`) FROM `test_model`;",
			},
		},
		// ??????
		{
			name: "alias",
			q:    NewSelector[TestModel](db).
				Select(C("Id").As("my_id"),
					Avg("Age").As("avg_age")),
			wantQuery: &Query{
				SQL: "SELECT `id` AS `my_id`,AVG(`age`) AS `avg_age` FROM `test_model`;",
			},
		},
		// WHERE ????????????
		{
			name: "where ignore alias",
			q:    NewSelector[TestModel](db).
				Where(C("Id").As("my_id").LT(100)),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model` WHERE `id` < ?;",
				Args: []any{100},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}

func TestSelector_Build(t *testing.T) {
	db := memoryDB(t)
	testCases := []struct {
		name      string
		q         QueryBuilder
		wantQuery *Query
		wantErr   error
	}{
		{
			// From ????????????
			name: "no from",
			q:    NewSelector[TestModel](db),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model`;",
			},
		},
		{
			// ?????? FROM
			name: "with from",
			q:    NewSelector[TestModel](db).From("`test_model_t`"),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model_t`;",
			},
		},
		{
			// ?????? FROM???????????????????????????
			name: "empty from",
			q:    NewSelector[TestModel](db).From(""),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_model`;",
			},
		},
		{
			// ?????? FROM????????????????????? DB
			name: "with db",
			q:    NewSelector[TestModel](db).From("`test_db`.`test_model`"),
			wantQuery: &Query{
				SQL: "SELECT * FROM `test_db`.`test_model`;",
			},
		},
		{
			// ??????????????????
			name: "single and simple predicate",
			q:    NewSelector[TestModel](db).From("`test_model_t`").
				Where(C("Id").EQ(1)),
			wantQuery: &Query{
				SQL:  "SELECT * FROM `test_model_t` WHERE `id` = ?;",
				Args: []any{1},
			},
		},
		{
			// ?????? predicate
			name: "multiple predicates",
			q: NewSelector[TestModel](db).
				Where(C("Age").GT(18), C("Age").LT(35)),
			wantQuery: &Query{
				SQL:  "SELECT * FROM `test_model` WHERE (`age` > ?) AND (`age` < ?);",
				Args: []any{18, 35},
			},
		},
		{
			// ?????? AND
			name: "and",
			q: NewSelector[TestModel](db).
				Where(C("Age").GT(18).And(C("Age").LT(35))),
			wantQuery: &Query{
				SQL:  "SELECT * FROM `test_model` WHERE (`age` > ?) AND (`age` < ?);",
				Args: []any{18, 35},
			},
		},
		{
			// ?????? OR
			name: "or",
			q:    NewSelector[TestModel](db).
				Where(C("Age").GT(18).Or(C("Age").LT(35))),
			wantQuery: &Query{
				SQL:  "SELECT * FROM `test_model` WHERE (`age` > ?) OR (`age` < ?);",
				Args: []any{18, 35},
			},
		},
		{
			// ?????? NOT
			name: "not",
			q:    NewSelector[TestModel](db).Where(Not(C("Age").GT(18))),
			wantQuery: &Query{
				// NOT ????????????????????????????????????????????? NOT ??????????????????
				SQL:  "SELECT * FROM `test_model` WHERE  NOT (`age` > ?);",
				Args: []any{18},
			},
		},
		{
			// ?????????
			name: "invalid column",
			q:    NewSelector[TestModel](db).Where(Not(C("Invalid").GT(18))),
			wantErr: errs.NewErrUnknownField("Invalid"),
		},
		{
			// ?????? RawExpr
			name: "raw expression",
			q:    NewSelector[TestModel](db).
				Where(Raw("`age` < ?", 18).AsPredicate()),
			wantQuery: &Query{
				SQL:  "SELECT * FROM `test_model` WHERE `age` < ?;",
				Args: []any{18},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, err := tc.q.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, query)
		})
	}
}


