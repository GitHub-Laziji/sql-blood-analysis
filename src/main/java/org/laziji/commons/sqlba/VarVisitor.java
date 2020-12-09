package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.ArrayList;
import java.util.List;

public class VarVisitor extends SQLASTVisitorAdapter {

    private List<Column> vars = new ArrayList<>();

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        return handle(x);
    }

    @Override
    public boolean visit(SQLPropertyExpr x) {
        return handle(x);
    }

    public List<Column> getVars() {
        return vars;
    }

    private boolean handle(SQLExpr expr) {
        if (expr.getParent() instanceof SQLPropertyExpr) {
            return true;
        }
        try {
            vars.add(Column.create(expr));
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}