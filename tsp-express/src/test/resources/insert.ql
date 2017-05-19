import com.alleyz.tsp.test.express.Bean;
import com.alleyz.tsp.test.express.OriBean;

Bean beans = new Bean();
# 设置要插入的表名
beans.setTableName("tbl_qc_name");
for(int i = 0; i < oriBeanList.size(); i++) {
    OriBean bean = (OriBean)oriBeanList.get(i);
    # 保留 level为01的以及userTxt包含抵消的信息
    if("01".equals(bean.getLevel()) && bean.getUserTxt().indexOf("抵消") >= 0){
        beans.getPropList().add(
            NewMap("row_id" : bean.getRowKey(), "level":bean.getLevel(), "allTxt":bean.getUserTxt())
        );
    }
}
return beans;
