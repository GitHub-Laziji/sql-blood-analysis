package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;

import java.util.*;

public class BloodParseUtils {

    public static Map<String, Set<String>> parserSelectQuery(String sql, String defaultSchema, String type) throws Exception {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, type);
        SQLStatement statement = statements.get(0);
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) statement;
        SQLSelectQuery select = sqlSelectStatement.getSelect().getQuery();
        return parserSelectQuery(select, defaultSchema);
    }

    public static Map<String, Set<String>> parserSelectQuery(SQLSelectQuery originSelectQuery, String defaultSchema) throws Exception {
        Map<String, Set<String>> columnMap = new HashMap<>();
        if (originSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock selectQuery = (SQLSelectQueryBlock) originSelectQuery;
            SQLTableSource originTable = selectQuery.getFrom();
            Map<String, Set<String>> exColumnMap = new HashMap<>();
            List<SQLSelectItem> columns = selectQuery.getSelectList();
            for (SQLSelectItem column : columns) {
                SQLExpr originExpr = column.getExpr();
                Column columnInfo = Column.create(column);
                if ("*".equals(columnInfo.getName())) {
                    exColumnMap.putAll(dfsSource(originTable, defaultSchema, columnInfo));
                    continue;
                }
                VarVisitor visitor = new VarVisitor();
                originExpr.accept(visitor);
                List<Column> vars = visitor.getVars();
                Set<String> from = new HashSet<>();
                for (Column var : vars) {
                    Map<String, Set<String>> source = dfsSource(originTable, defaultSchema, var);
                    if (source.get(var.getName()) == null) {
                        throw new Exception(String.format("表中不存在 %s 字段", var.getName()));
                    }
                    from.addAll(source.get(var.getName()));
                }
                columnMap.put(columnInfo.getAlias(), from);
            }
            for (Map.Entry<String, Set<String>> entry : exColumnMap.entrySet()) {
                String key = entry.getKey();
                if (columnMap.get(key) == null) {
                    columnMap.put(key, exColumnMap.get(key));
                }
            }
        } else if (originSelectQuery instanceof SQLUnionQuery) {
            SQLUnionQuery selectQuery = (SQLUnionQuery) originSelectQuery;
            columnMap.putAll(parserSelectQuery(selectQuery.getLeft(), defaultSchema));
            for (Map.Entry<String, Set<String>> entry : parserSelectQuery(selectQuery.getRight(), defaultSchema).entrySet()) {
                if (columnMap.get(entry.getKey()) != null) {
                    columnMap.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    columnMap.put(entry.getKey(), entry.getValue());
                }
            }
        } else {
            throw new Exception(String.format("无法解析 %s 类型语句", originSelectQuery.getClass().getSimpleName()));
        }
        return columnMap;
    }

    static String transName(String name) {
        char ch = name.charAt(0);
        if (ch == '`' | ch == '\'' || ch == '"') {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    private static Map<String, Set<String>> dfsSource(SQLTableSource originTable, String defaultSchema, Column var) throws Exception {
        Map<String, Set<String>> vars = new HashMap<>();
        if (originTable instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource) originTable;
            Table tableInfo = Table.create(table, defaultSchema);
            if (tableInfo.contains(var)) {
                vars.put(var.getAlias(), Collections.singleton(tableInfo.toString() + "." + var.getName()));
            }
        } else if (originTable instanceof SQLJoinTableSource) {
            SQLJoinTableSource table = ((SQLJoinTableSource) originTable);
            vars.putAll(dfsSource(table.getLeft(), defaultSchema, var));
            Map<String, Set<String>> rightSource = dfsSource(table.getRight(), defaultSchema, var);
            for (Map.Entry<String, Set<String>> entry : rightSource.entrySet()) {
                String key = entry.getKey();
                if (vars.get(key) != null) {
                    vars.get(key).addAll(entry.getValue());
                } else {
                    vars.put(key, entry.getValue());
                }
            }
        } else if (originTable instanceof SQLSubqueryTableSource || originTable instanceof SQLUnionQueryTableSource) {
            Table tableInfo = Table.create(originTable, defaultSchema);
            if (tableInfo.contains(var)) {
                Map<String, Set<String>> columnMap;
                if (originTable instanceof SQLSubqueryTableSource) {
                    columnMap = parserSelectQuery(((SQLSubqueryTableSource) originTable).getSelect().getQuery(), defaultSchema);
                } else {
                    columnMap = parserSelectQuery(((SQLUnionQueryTableSource) originTable).getUnion(), defaultSchema);
                }
                if (!var.getName().equals("*")) {
                    if (columnMap.get(var.getName()) != null) {
                        vars.put(var.getName(), columnMap.get(var.getName()));
                    } else if (columnMap.get("*") != null) {
                        Set<String> set = new HashSet<>();
                        vars.put(var.getName(), set);
                        for (String from : columnMap.get("*")) {
                            set.add(from.replace("*", var.getName()));
                        }
                    }
                } else {
                    vars.putAll(columnMap);
                }
            }
        }
        return vars;
    }

}
