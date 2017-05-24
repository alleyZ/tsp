package com.alleyz.tsp.topo.utils;

import com.alleyz.tsp.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by alleyz on 2017/5/23.
 *
 */
public class JdbcHelper {
    private static JdbcHelper instance = new JdbcHelper();
    private static Logger logger = LoggerFactory.getLogger(JdbcHelper.class);
    private JdbcHelper(){
        try {
            this.connection = DriverManager.getConnection(
                    ConfigUtil.getStrVal("jdbc.url"),
                    ConfigUtil.getStrVal("jdbc.userName"),
                    ConfigUtil.getStrVal("jdbc.password"));
        }catch (SQLException e){
            logger.error("connect db failure!", e);
        }
    }
    public static JdbcHelper getInstance(){
        return instance;
    }
    static {
        try {
            Class.forName(ConfigUtil.getStrVal("jdbc.driver"));
        }catch (ClassNotFoundException e){
            logger.error("can`t find oracle driver class", e);
        }
    }
    private Connection connection;

    public int insert(String sql, String ... params) throws SQLException{
        try(PreparedStatement ps = connection.prepareStatement(sql)){
            for(int i = 0; i< params.length; i ++){
                ps.setString(i, params[i]);
            }
            return ps.executeUpdate();
        }
    }

}
