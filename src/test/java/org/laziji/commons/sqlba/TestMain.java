package org.laziji.commons.sqlba;

import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSON;
import org.junit.Test;

public class TestMain {

    @Test
    public void test() throws Exception {
        System.out.println(JSON.toJSONString(BloodParseUtils.parserSelectQuery("select `id`,\n" +
                "       b.dtsid,\n" +
                "       b.type,\n" +
                "       (case\n" +
                "          when id > 1000 && mh > 1000 then 1\n" +
                "          else 0 end) 'cs'\n" +
                "from (select *\n" +
                "      from (select k.dsi1 dtsid from (select dsi1,dsi2 from md) k) as m\n" +
                "             join dts d on m.dtsid = d.id) b\n" +
                "       join md on b.id = md.id\n" +
                "where b.type = 'xx'", "default", JdbcConstants.MYSQL), true));
    }

}
