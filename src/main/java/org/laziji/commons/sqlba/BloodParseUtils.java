package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.*;

public class BloodParseUtils {

    public static Map<String, Set<String>> parserSelectQuery(String sql, String type) throws Exception {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, type);
        SQLStatement statement = statements.get(0);
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery select = sqlSelectStatement.getSelect().getQuery();
        return parserSelectQuery(select);
    }

    public static Map<String, Set<String>> parserSelectQuery(SQLSelectQuery originSelectQuery) throws Exception {
        if (originSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock selectQuery = (SQLSelectQueryBlock) originSelectQuery;
            SQLTableSource originTable = selectQuery.getFrom();
            Map<String, Set<String>> columnMap = new HashMap<>();
            Map<String, Set<String>> exColumnMap = new HashMap<>();
            List<SQLSelectItem> columns = selectQuery.getSelectList();
            for (SQLSelectItem column : columns) {
                String columnAlias = column.getAlias();
                SQLExpr originExpr = column.getExpr();
                if (originExpr instanceof SQLPropertyExpr) {
                    SQLPropertyExpr expr = (SQLPropertyExpr) originExpr;
                    if (expr.getName().equals("*")) {
                        exColumnMap.putAll(dfsSource(originTable, SQLUtils.toSQLString(expr.getOwner()), "*"));
                        continue;
                    }
                    columnAlias = columnAlias == null ? expr.getName() : columnAlias;
                } else if (originExpr instanceof SQLAllColumnExpr) {
                    exColumnMap.putAll(dfsSource(originTable, null, "*"));
                    continue;
                } else {
                    columnAlias = columnAlias == null ? SQLUtils.toSQLString(originExpr) : columnAlias;
                }
                columnAlias = transName(columnAlias);
                VarVisitor visitor = new VarVisitor();
                originExpr.accept(visitor);
                List<String[]> vars = visitor.getVars();
                Set<String> from = new HashSet<>();
                for (String[] var : vars) {
                    String owner = var[0] == null ? null : transName(var[0]);
                    String name = transName(var[1]);
                    Map<String, Set<String>> source = dfsSource(originTable, owner, name);
                    if (source.get(name) == null) {
                        throw new Exception(String.format("表中不存在%s字段", name));
                    }
                    from.addAll(source.get(name));
                }
                columnMap.put(columnAlias, from);
            }
            for (Map.Entry<String, Set<String>> entry : exColumnMap.entrySet()) {
                String key = entry.getKey();
                if (columnMap.get(key) == null) {
                    columnMap.put(key, exColumnMap.get(key));
                }
            }
            return columnMap;
        } else if (originSelectQuery instanceof SQLUnionQuery) {
            return new HashMap<>();
        } else {
            throw new Exception(String.format("无法解析 %s 类型语句", originSelectQuery.getClass().getSimpleName()));
        }
    }

    private static Map<String, Set<String>> dfsSource(SQLTableSource originTable, String owner, String name) throws Exception {
        Map<String, Set<String>> vars = new HashMap<>();
        if (originTable instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource) originTable;
            String tableName = SQLUtils.toSQLString(table.getExpr());
            String tableAlias = transName(table.getAlias() == null ? tableName : table.getAlias());
            if (owner == null || tableAlias.equals(owner)) {
                vars.put(name, Collections.singleton(tableName + "." + name));
            }
        } else if (originTable instanceof SQLJoinTableSource) {
            SQLJoinTableSource table = ((SQLJoinTableSource) originTable);
            vars.putAll(dfsSource(table.getLeft(), owner, name));
            Map<String, Set<String>> rightSource = dfsSource(table.getRight(), owner, name);
            for (Map.Entry<String, Set<String>> entry : rightSource.entrySet()) {
                String key = entry.getKey();
                if (vars.get(key) != null) {
                    vars.get(key).addAll(entry.getValue());
                } else {
                    vars.put(key, entry.getValue());
                }
            }
        } else if (originTable instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource table = ((SQLSubqueryTableSource) originTable);
            String tableAlias = transName(table.getAlias());
            if (owner == null || tableAlias.equals(owner)) {
                Map<String, Set<String>> columnMap = parserSelectQuery(table.getSelect().getQuery());
                if (!name.equals("*")) {
                    if (columnMap.get(name) != null) {
                        vars.put(name, columnMap.get(name));
                    } else if (columnMap.get("*") != null) {
                        Set<String> set = new HashSet<>();
                        vars.put(name, set);
                        for (String var : columnMap.get("*")) {
                            set.add(var.replace("*", name));
                        }
                    }
                } else {
                    vars.putAll(columnMap);
                }
            }
        }
        return vars;
    }

    private static String transName(String name) {
        char ch = name.charAt(0);
        if (ch == '`' | ch == '\'' || ch == '"') {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }
}
