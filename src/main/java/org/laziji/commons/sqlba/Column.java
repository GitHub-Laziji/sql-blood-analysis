package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;

public class Column {

    private Table table;

    private String name;

    private String alias;

    public static Column create(SQLSelectItem item) throws Exception {
        Column column = create(item.getExpr());
        if (item.getAlias() != null) {
            column.alias = BloodParseUtils.transName(item.getAlias());
        }
        return column;
    }

    public static Column create(SQLExpr expr) throws Exception {
        if (expr == null) {
            return null;
        }
        Column column = new Column();
        if (expr instanceof SQLIdentifierExpr) {
            column.name = BloodParseUtils.transName(((SQLIdentifierExpr) expr).getName());
            column.alias = column.name;
        } else if (expr instanceof SQLPropertyExpr) {
            column.name = BloodParseUtils.transName(((SQLPropertyExpr) expr).getName());
            column.alias = column.name;
            column.table = Table.create(((SQLPropertyExpr) expr).getOwner(), null);
        } else if (expr instanceof SQLAllColumnExpr) {
            column.alias = column.name = "*";
        } else {
            column.alias = SQLUtils.toSQLString(expr);
        }
        return column;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        String tableName = table.toString();
        if (tableName == null) {
            return name;
        }
        return tableName + "." + name;
    }
}
