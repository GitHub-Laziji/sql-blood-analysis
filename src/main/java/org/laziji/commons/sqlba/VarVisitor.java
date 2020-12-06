package org.laziji.commons.sqlba;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

import java.util.ArrayList;
import java.util.List;

public class VarVisitor extends SQLASTVisitorAdapter {

    private List<String[]> vars = new ArrayList<>();

    @Override
    public boolean visit(SQLIdentifierExpr x) {
        if (x.getParent() instanceof SQLPropertyExpr) {
            return true;
        }
        vars.add(new String[]{null, x.getName()});
        return true;
    }

    @Override
    public boolean visit(SQLPropertyExpr x) {
        String owner = SQLUtils.toSQLString(x.getOwner());
        String name = x.getName();
        vars.add(new String[]{owner, name});
        return true;
    }

    public List<String[]> getVars() {
        return vars;
    }
}