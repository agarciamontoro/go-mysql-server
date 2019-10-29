package analyzer

import (
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestAssignCatalog(t *testing.T) {
	require := require.New(t)
	f := getRule("assign_catalog")

	db := memory.NewDatabase("foo")
	c := sql.NewCatalog()
	c.AddDatabase(db)

	a := NewDefault(c)
	a.Catalog.IndexRegistry = sql.NewIndexRegistry()

	tbl := memory.NewTable("foo", nil)

	node, err := f.Apply(sql.NewEmptyContext(), a,
		plan.NewCreateIndex("", plan.NewResolvedTable(tbl), nil, "", make(map[string]string)))
	require.NoError(err)

	ci, ok := node.(*plan.CreateIndex)
	require.True(ok)
	require.Equal(c, ci.Catalog)
	require.Equal("foo", ci.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a,
		plan.NewDropIndex("foo", plan.NewResolvedTable(tbl)))
	require.NoError(err)

	di, ok := node.(*plan.DropIndex)
	require.True(ok)
	require.Equal(c, di.Catalog)
	require.Equal("foo", di.CurrentDatabase)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewShowIndexes(db, "table-test", nil))
	require.NoError(err)

	si, ok := node.(*plan.ShowIndexes)
	require.True(ok)
	require.Equal(db, si.Database())
	require.Equal(c.IndexRegistry, si.Registry)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewShowProcessList())
	require.NoError(err)

	pl, ok := node.(*plan.ShowProcessList)
	require.True(ok)
	require.Equal(db.Name(), pl.Database)
	require.Equal(c.ProcessList, pl.ProcessList)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewShowDatabases())
	require.NoError(err)
	sd, ok := node.(*plan.ShowDatabases)
	require.True(ok)
	require.Equal(c, sd.Catalog)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewLockTables(nil))
	require.NoError(err)
	lt, ok := node.(*plan.LockTables)
	require.True(ok)
	require.Equal(c, lt.Catalog)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewUnlockTables())
	require.NoError(err)
	ut, ok := node.(*plan.UnlockTables)
	require.True(ok)
	require.Equal(c, ut.Catalog)

	mockSubquery := plan.NewSubqueryAlias("mock", plan.NewResolvedTable(tbl))
	mockView := plan.NewCreateView(db, "", nil, mockSubquery, false)
	node, err = f.Apply(sql.NewEmptyContext(), a, mockView)
	require.NoError(err)
	cv, ok := node.(*plan.CreateView)
	require.True(ok)
	require.Equal(c, cv.Catalog)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewDropView(nil, false))
	require.NoError(err)
	dv, ok := node.(*plan.DropView)
	require.True(ok)
	require.Equal(c, dv.Catalog)

	node, err = f.Apply(sql.NewEmptyContext(), a, plan.NewShowTables(db, false))
	require.NoError(err)
	st, ok := node.(*plan.ShowTables)
	require.True(ok)
	require.Equal(c, st.Catalog)
}
