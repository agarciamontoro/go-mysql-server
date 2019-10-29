package plan

import (
	"sort"
	"testing"

	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestShowTables(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	catalog := sql.NewCatalog()

	unresolvedShowTables := NewShowTables(sql.UnresolvedDatabase(""), false)
	unresolvedShowTables.Catalog = catalog

	require.False(unresolvedShowTables.Resolved())
	require.Nil(unresolvedShowTables.Children())

	tables := []string{"test1", "test2", "test3"}
	views := []string{"view1", "view2"}

	db := memory.NewDatabase("test")
	for _, table := range tables {
		db.AddTable(table, memory.NewTable(table, nil))
	}
	for _, view := range views {
		err := catalog.ViewRegistry.Register(db.Name(), sql.NewView(view, nil))
		require.NoError(err)
	}

	resolvedShowTables := NewShowTables(db, true)
	resolvedShowTables.Catalog = catalog

	require.True(resolvedShowTables.Resolved())
	require.Nil(resolvedShowTables.Children())

	iter, err := resolvedShowTables.RowIter(ctx)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	var actualTables []string
	var actualViews []string
	for _, row := range rows {
		name, ok := row[0].(string)
		require.True(ok)
		switch row[1] {
		case "BASE TABLE":
			actualTables = append(actualTables, name)
		case "VIEW":
			actualViews = append(actualViews, name)
		default:
			require.FailNow("only values 'BASE TABLE' and 'VIEW' are expected")
		}
	}

	sort.Strings(tables)
	sort.Strings(actualTables)
	require.Equal(tables, actualTables)

	sort.Strings(views)
	sort.Strings(actualViews)
	require.Equal(views, actualViews)
}
