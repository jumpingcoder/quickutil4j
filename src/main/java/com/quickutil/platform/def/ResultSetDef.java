package com.quickutil.platform.def;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class ResultSetDef {
    public Connection connection;
    public List<String> columnName;
    public ResultSet rs;
    public PreparedStatement ps;

    public ResultSetDef(Connection connection, PreparedStatement ps,  ResultSet rs,  List<String> columnName){
        this.connection = connection;
        this.columnName = columnName;
        this.rs = rs;
        this.ps = ps;
    }

    public void close(){
        try {
            if (rs != null)
                rs.close();
            if (ps != null)
                ps.close();
            if (connection != null)
                connection.close();
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}
