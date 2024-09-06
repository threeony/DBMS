package org.example;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MetadataManager {

    static String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static String DB_URL = "jdbc:mysql://localhost:3306/metadata";
    static String USER = "root";
    static String PWD = "!qpla1029M";


    public class AttributesInformation{
        String attribute;
        Integer length;

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attribute) {
            this.attribute = attribute;
        }

        public Integer getLength() {
            return length;
        }

        public void setLength(Integer length) {
            this.length = length;
        }
    }

    void createTable(BufferedReader br) throws IOException {

        System.out.println("Input the table name.");
        System.out.print(">> ");
        String name = br.readLine().toLowerCase();

        List<String> columnInfo = new ArrayList<>();
        System.out.println("Input column info(column name, data type).");
        System.out.println("Press enter when you're done.");
        String line;
        while(!(line=br.readLine()).equals("")){
            columnInfo.add(line);
        }

        System.out.println("Input the primary key.");
        System.out.print(">> ");
        String primaryKey = br.readLine();

        int length = insertAttributeMetadata(name, columnInfo);
        insertRelationMetadata(name, columnInfo.size(), length, primaryKey);
        //header에 free list pointer(다음 레코드를 삽입할 주소)를 포함하여 파일에 작성
        FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir")+"\\"+name+".txt");
        //free list pointer
        String header = String.format("%-"+length+"s",length);
        fos.write(header.getBytes());

        fos.close();
    }

    void insertRelationMetadata(String tableName, Integer columnNo, Integer length, String primaryKey) {
        try{
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(DB_URL, USER, PWD);

            String sql = "INSERT INTO relation_metadata (relation_name, number_of_attributes, location, length, primary_key)" +
                    "VALUES (?, ?, ?, ?, ?)";

            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, tableName);
            pstmt.setInt(2, columnNo);
            pstmt.setString(3, System.getProperty("user.dir")+"\\"+tableName+".txt");
            pstmt.setInt(4, length);
            pstmt.setString(5, primaryKey);

            pstmt.executeUpdate();

            pstmt.close();
            conn.close();
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    int insertAttributeMetadata(String tableName, List<String> columnInfo){
        int length = 0;

        try{
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(DB_URL, USER, PWD);

            String sql = "INSERT INTO attribute_metadata (relation_name, attribute_name, domain_type, position, length)" +
                    "VALUES (?, ?, ?, ?, ?)";

            int i=0;
            for(String column: columnInfo) {
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, tableName);
                pstmt.setString(2, column.split(",")[0]);
                pstmt.setString(3, column.split(", ")[1].split("\\(")[0]);
                pstmt.setInt(4, i);
                int columnLength = Integer.parseInt(column.split(", ")[1].split("\\(")[1].split("\\)")[0]);
                length = length + columnLength;
                pstmt.setInt(5, columnLength);

                pstmt.executeUpdate();
                pstmt.close();
                i++;
            }
            conn.close();
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }

        return length;
    }

    public String getMethod(String tableName, String column){
        String result = null;
        try{
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(DB_URL, USER, PWD);

            String sql = "SELECT * FROM relation_metadata WHERE relation_name = \'"+tableName+"\'";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while(rs.next()){
                result = rs.getString(column);
            }

            rs.close();
            stmt.close();
            conn.close();
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    public ArrayList<AttributesInformation> getAttributes(String tableName){
        ArrayList<AttributesInformation> attributesInformations = new ArrayList<>();
        try{
            Class.forName(JDBC_DRIVER);
            Connection conn = DriverManager.getConnection(DB_URL, USER, PWD);

            String sql = "SELECT * FROM attribute_metadata where relation_name = \'"+tableName+"\'";
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()){
                AttributesInformation attribute = new AttributesInformation();
                attribute.setAttribute(rs.getString("attribute_name"));
                attribute.setLength(rs.getInt("length"));
                attributesInformations.add(attribute);
            }

            rs.close();
            stmt.close();
            conn.close();
        }catch (SQLException se){
            se.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }

        return attributesInformations;
    }
}