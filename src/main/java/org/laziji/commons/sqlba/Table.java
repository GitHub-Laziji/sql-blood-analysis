package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;

import java.util.Objects;

public class Table {

    private String schemaName;

    private String name;

    private String alias;

    public static Table create(SQLTableSource source, String defaultSchema) {
        Table table = new Table();
        table.schemaName = defaultSchema;
        table.alias = BloodParseUtils.transName(source.getAlias());
        return table;
    }

    public static Table create(SQLExprTableSource source, String defaultSchema) throws Exception {
        Table table = create(source.getExpr(), defaultSchema);
        if (source.getAlias() != null) {
            table.alias = BloodParseUtils.transName(source.getAlias());
        }
        if (table.schemaName == null) {
            table.schemaName = defaultSchema;
        }
        return table;
    }

    public static Table create(SQLExpr expr, String defaultSchema) throws Exception {
        if (expr == null) {
            return null;
        }
        Table table = new Table();
        if (expr instanceof SQLIdentifierExpr) {
            table.name = ((SQLIdentifierExpr) expr).getName();
        } else if (expr instanceof SQLPropertyExpr) {
            table.name = ((SQLPropertyExpr) expr).getName();
            if (((SQLPropertyExpr) expr).getOwner() != null) {
                table.schemaName = SQLUtils.toSQLString(((SQLPropertyExpr) expr).getOwner());
            }
        } else {
            throw new Exception("表名不合法");
        }
        if (table.schemaName == null) {
            table.schemaName = defaultSchema;
        }
        table.alias = table.name = BloodParseUtils.transName(table.name);
        if (table.schemaName != null) {
            table.schemaName = BloodParseUtils.transName(table.schemaName);
        }
        return table;
    }

    public boolean contains(Column column) {
        return column.getTable() == null ||
                column.getTable().getSchemaName() == null &&
                        Objects.equals(column.getTable().getName(), alias) ||
                Objects.equals(column.getTable().getSchemaName(), schemaName) &&
                        Objects.equals(column.getTable().getName(), alias);
    }


    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
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
        if (name == null) {
            return null;
        }
        if (schemaName != null) {
            return schemaName + "." + name;
        }
        return name;
    }
}
