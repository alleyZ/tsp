package com.alleyz.tsp.pretreat;

import com.alleyz.tsp.constant.Constant;
import com.alleyz.tsp.pretreat.jdbc.JdbcUtil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.alleyz.tsp.constant.Constant.DELIMITER_BLOCK;
import static com.alleyz.tsp.constant.Constant.DELIMITER_FIELDS;
import static com.alleyz.tsp.pretreat.config.Const.*;

/**
 * Created by alleyz on 2017/5/17.
 *
 */
public class ProcessFile {

    public static void main(String[] args) {
        try {
            Map<String, Map<String, String>> codeGroups = getCode(CT_AREA_CODE, CT_AREA_TREE, CT_BUSINESS_TYPE,
                    CT_CUST_BRAND, CT_CUST_LEVEL, CT_DIRECTION, CT_NET_TYPE, CT_SATISFACTION, CT_SHEET_TYPE);
            String path = "G:\\data\\kafka-reader";
            File paths =new File(path);
            File[] files = paths.listFiles();
            for(File file : files) {
                String fileName = (file.getPath()+ "_tra");
                System.out.println(fileName);
                File outFile = new File(fileName);
                outFile.createNewFile();
                int i = 0;
                try(
                        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outFile)));
                ){
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split("_|\\||#");
                        String date = values[1];
                        String year = date.substring(0, 4);
                        String month = date.substring(0, 6);
                        String day = date.substring(0, 8);
                        String hour = (i % 24)  + "";
                        if((hour).length() == 1 ) {
                            hour = "0" + hour;
                        }
                        hour = day + hour;
                        StringBuilder builder = new StringBuilder(values[0]);
                        builder.append(Constant.DELIMITER_BLOCK)
                                .append(values[17]).append(i).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_AREA_CODE).get(values[3])).append(DELIMITER_FIELDS)
                                .append(values[4]).append(DELIMITER_FIELDS)
                                .append(values[6]).append(DELIMITER_FIELDS)
                                .append(values[7]).append(DELIMITER_FIELDS)
                                .append(values[6]).append(DELIMITER_FIELDS)
                                .append(values[9]).append(DELIMITER_FIELDS)
                                .append(year).append(DELIMITER_FIELDS)
                                .append(month).append(DELIMITER_FIELDS)
                                .append(getWeek(day)).append(DELIMITER_FIELDS)
                                .append(day).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_AREA_TREE).get(values[15])).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_CUST_BRAND).get(values[14])).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_SATISFACTION).get(values[16])).append(DELIMITER_FIELDS)
                                .append("que").append(i).append(DELIMITER_FIELDS)
                                .append("serv").append(i).append(DELIMITER_FIELDS)
                                .append(values[18]).append(DELIMITER_FIELDS)
                                .append(values[2]).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_NET_TYPE).get(values[8])).append(DELIMITER_FIELDS)
                                .append("recn").append(i).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_BUSINESS_TYPE).get(values[12])).append(DELIMITER_FIELDS)
                                .append(codeGroups.get(CT_CUST_LEVEL).get(values[13])).append(DELIMITER_FIELDS)
                                .append("呼出".equals(values[5]) ? "1" : "0").append(i).append(DELIMITER_FIELDS)
                                .append("4k").append(DELIMITER_FIELDS)
                                .append("16bit").append(DELIMITER_FIELDS)
                                .append("vox").append(DELIMITER_FIELDS)
                                .append(values[11]).append(DELIMITER_FIELDS)
                                .append(values[10]).append(DELIMITER_FIELDS)
                                .append(hour);
                        String txt = values[19];
                        String[] txtes = txt.split("。。|！。|？。");
                        StringBuilder userTxt = new StringBuilder();
                        StringBuilder agentTxt = new StringBuilder();
                        for(int j = 0; j < txtes.length; j++) {
                            if(j % 2 == 0) {
                                agentTxt.append(txtes[j]);
                            }else{
                                userTxt.append(txtes[j]);
                            }
                        }
                        builder.append(DELIMITER_BLOCK)
                                .append(userTxt).append(DELIMITER_FIELDS)
                                .append(agentTxt).append(DELIMITER_FIELDS)
                                .append(txt);
                        bw.write(builder.toString() + "\r\n");
                        if((i+1) % 500 == 0)
                            bw.flush();
                    }
                }
            }
            System.out.println("over ~~~");
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String,Map<String,String>> getCode(String ... types) throws Exception{
        Map<String,Map<String,String>> codeGroups = new HashMap<>();
        String sql;
        for(String type : types) {
            if(type.equals(CT_AREA_TREE)) {
                sql = "select code_value, code_name from tbl_sys_tree_code where code_type = ?";
            }else{
                sql = "select g.code_value, g.code_name from tbl_sys_general_code g where g.enabled='1' and g.code_type=?";
            }
            Map<String,String> codeValues = JdbcUtil.getInstance().query(sql, new Object[]{type}, rs -> {
                Map<String,String> cvMap = new HashMap<String, String>();
                while (rs.next()) {
                    cvMap.put(rs.getString("code_name"), rs.getString("code_value"));
                }
                return cvMap;
            });
            codeGroups.put(type, codeValues);
        }
        return codeGroups;
    }

    static String getWeek(String day) throws Exception{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date cur = sdf.parse(day);
        Calendar cal = Calendar.getInstance();
        cal.setTime(cur);
        return cal.get(Calendar.WEEK_OF_YEAR) + "";
    }
}
