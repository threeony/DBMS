package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    static final MetadataManager metadataManager = new MetadataManager();
    static final DatabaseManager dbManager = new DatabaseManager();

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.println("Select a command you want to execute.");
            System.out.println("(1.CREATE TABLE  2.INSERT  3.DELETE  4.SELECT  5.JOIN  6.EXIT)");

            System.out.print(">> ");
            String command = br.readLine();

            switch (Integer.parseInt(command)) {
                case 1:
                    metadataManager.createTable(br);
                    break;
                case 2:
                    System.out.println("Enter a table for the record to be inserted.");
                    System.out.print(">> ");
                    String table = br.readLine();
                    dbManager.insertRecord(br, table.toLowerCase());
                    break;
                case 3:
                    System.out.println("Enter a table for the record to be deleted.");
                    System.out.print(">> ");
                    String table2 = br.readLine();
                    dbManager.deleteRecord(br, table2.toLowerCase());
                    break;
                case 4:
                    System.out.println("Enter a table for the record to be selected.");
                    System.out.print(">> ");
                    String table3 = br.readLine();
                    dbManager.selectRecord(br, table3.toLowerCase());
                    break;
                case 5:
                    System.out.println("Enter first table for the record to be selected.");
                    System.out.print(">> ");
                    String firstTable = br.readLine();
                    System.out.println("Enter the column of the first table.");
                    System.out.print(">> ");
                    String column1 = br.readLine();

                    System.out.println("Enter second table for the record to be selected.");
                    System.out.print(">> ");
                    String secondTable = br.readLine();
                    System.out.println("Enter the column of the second table.");
                    System.out.print(">> ");
                    String column2 = br.readLine();

                    dbManager.join(firstTable, column1, secondTable, column2);
                    break;
                case 6:
                    br.close();
                    return;
                default:
                    System.out.println("Invalid Option");
                    break;
            }
        }
    }
}