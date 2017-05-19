package com.alleyz.tsp.test.express;

import com.ql.util.express.DefaultContext;
import com.ql.util.express.ExpressRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by alleyz on 2017/5/18.
 */
public class Test {
    public static void main(String[] args) {
        try {
            String ql = readQl("/insert.ql");
            System.out.println(ql);
            ExpressRunner runner = new ExpressRunner();
            DefaultContext<String, Object> context = new DefaultContext<>();
            context.put("oriBeanList", list());
            List<String> errList = new ArrayList<>();
            Bean bean = (Bean) runner.execute(ql, context, errList, true, false);
            System.out.println("insert table = " + bean.getTableName());
            bean.getPropList().forEach(System.out::println);
            errList.forEach(System.out::println);
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static List<OriBean> list(){
        List<OriBean> beans = new ArrayList<>();
        for(int i = 0; i < 100; i++){
            OriBean bean = new OriBean();
            bean.setRowKey(i + "");
            if(i % 6 ==0){
                bean.setLevel("01");
            }else{
                bean.setLevel(i + "");
            }

            if(i % 3 == 0)
                bean.setUserTxt("抵消可是可是开始看"+ i);
            else
                bean.setUserTxt("阿里本科生可是可是");

            beans.add(bean);
        }

        return  beans;
    }

    public static String readQl(String qlFile) {
        StringBuilder builder = new StringBuilder();
        try(BufferedReader br = new BufferedReader(new InputStreamReader(Test.class.getResourceAsStream(qlFile)))){
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if(line.indexOf("#") == 0) continue;
                if(line.length() > 0)
                    builder.append(line);
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return builder.toString();
    }
}
