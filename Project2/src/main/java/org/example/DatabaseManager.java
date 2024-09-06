package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class DatabaseManager {
    static final MetadataManager metadataManager = new MetadataManager();

    public void insertRecord(BufferedReader reader, String tableName) throws IOException {
        ArrayList<MetadataManager.AttributesInformation> attributesInformations;
        attributesInformations = metadataManager.getAttributes(tableName);
        System.out.println("Enter the values of the record in order.");
        for (MetadataManager.AttributesInformation information : attributesInformations) {
            System.out.print("|" + String.format("%-" + information.getLength() + "s", information.getAttribute()));
        }
        System.out.println("|");

        System.out.print(">> ");
        String values = reader.readLine();

        int i = 0;
        String formattedValue = "";
        for (MetadataManager.AttributesInformation information : attributesInformations) {
            if (values.split(",")[i].strip().length() > information.getLength()) {
                System.out.println("Exceeded length.");
                return;
            }
            formattedValue = formattedValue + String.format("%-" + information.getLength() + "s", values.split(",")[i].strip());
            i++;
        }

        String location = metadataManager.getMethod(tableName, "location");
        Integer length = Integer.parseInt(metadataManager.getMethod(tableName, "length"));

        //header에서 free list를 통해 레코드를 삽입할 위치 및 해당 블록 확인
        RandomAccessFile raf = new RandomAccessFile(location, "rw");
        Integer start = 0;
        byte[] bytes = new byte[length * 3 +1];

        raf.seek(start);
        raf.read(bytes);
        String block= new String(bytes, "UTF-8");

        String header = block.substring(0, length);
        Integer pointer = Integer.parseInt(header.trim());

        if(pointer < length*3){ //같은 블록에 있을 경우
            if(block.substring(pointer,pointer+length).matches("^[0-9\\s]+$")){
                header = block.substring(pointer,pointer+length);
            }else{
                header = String.format("%-" + length + "s", pointer + length);
            }
            String afterInsert = header+block.substring(length,pointer)+formattedValue+block.substring(pointer+length);

            raf.seek(0);
            raf.write(afterInsert.getBytes("UTF-8"));
        }else{
            bytes = new byte[length * 3 +1];
            start = (length*3)*(pointer/(length*3));
            raf.seek(start);
            raf.read(bytes);
            block= new String(bytes, "UTF-8");

            if(block.substring(pointer-start,pointer-start+length).matches("^[0-9\\s]+$")){
                header = block.substring(pointer-start,pointer-start+length);
            }else{
                header = String.format("%-" + length + "s", pointer + length);
            }
            String afterInsert = block.substring(0,pointer-start)+formattedValue+block.substring(pointer-start+length);
            raf.seek(start);
            raf.write(afterInsert.getBytes("UTF-8"));

            bytes = new byte[length * 3 +1];
            raf.seek(0);
            raf.read(bytes);
            block= new String(bytes, "UTF-8");
            afterInsert = header + block.substring(length);
            raf.seek(0);
            raf.write(afterInsert.getBytes("UTF-8"));
        }
    }

    public void selectRecord(BufferedReader reader, String tableName) throws IOException {
        System.out.println("Select an option.");
        System.out.println("(1. Select 1 record.  2. Select all records)");
        System.out.print(">> ");
        Integer option = Integer.parseInt(reader.readLine());

        ArrayList<MetadataManager.AttributesInformation> attributesInformations;
        attributesInformations = metadataManager.getAttributes(tableName);
        String location = metadataManager.getMethod(tableName, "location");
        Integer length = Integer.parseInt(metadataManager.getMethod(tableName, "length"));

        BufferedReader fr = new BufferedReader(new FileReader(location), length*3);
        switch(option){
            case 1:
                String primaryKey = metadataManager.getMethod(tableName, "primary_key");
                System.out.println("Input the primary key of the record.");
                System.out.print(">> ");
                String primaryKeyValue = reader.readLine();


                for(MetadataManager.AttributesInformation information: attributesInformations){
                    System.out.print("|"+String.format("%-"+information.getLength()+"s", information.getAttribute()));
                }
                System.out.println("|");

                char[] buffer = new char[length*3];
                while(true) {
                    int charsRead = fr.read(buffer, 0, length*3);
                    if(charsRead == -1)
                        break;

                    String block1 = new String(buffer, 0, charsRead);
                    if(block1.contains(primaryKeyValue)){
                        String[] subStringArray = block1.split("(?<=\\G.{" + length + "})");
                        for(String string : subStringArray){
                            if(string.contains(primaryKeyValue)){
                                String str = string;
                                String checkPrimaryKey = "";
                                for(MetadataManager.AttributesInformation information: attributesInformations){
                                    if(information.getAttribute().toString().equals(primaryKey)) {
                                        if(checkPrimaryKey.substring(0, information.getLength().intValue()).trim().equals(primaryKeyValue)) {
                                            for(MetadataManager.AttributesInformation information2: attributesInformations){
                                                System.out.print("|" + string.substring(0, information2.getLength()));
                                                string = string.substring(information2.getLength());
                                            }
                                            System.out.println("|");
                                            return;
                                        }
                                    }
                                    else{
                                        checkPrimaryKey = str.substring(information.getLength());
                                        str = checkPrimaryKey;
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            case 2:
                for(MetadataManager.AttributesInformation information: attributesInformations){
                    System.out.print("|"+String.format("%-"+information.getLength()+"s", information.getAttribute()));
                }
                System.out.println("|");

                buffer = new char[length*3];
                while(true) {
                    int charsRead = fr.read(buffer, 0, length*3);
                    if(charsRead == -1)
                        break;

                    String block2 = new String(buffer, 0, charsRead);
                    String[] subStringArray = block2.split("(?<=\\G.{" + length + "})");
                    for(String string : subStringArray){
                        if(!string.trim().isEmpty()) {
                            if (!(Pattern.matches("^[0-9\\s]+$", string))) {
                                for (MetadataManager.AttributesInformation information : attributesInformations) {
                                    System.out.print("|" + string.substring(0, information.getLength()));
                                    string = string.substring(information.getLength());
                                }
                                System.out.println("|");
                            }
                        }
                    }
                }
                break;
            default:
                System.out.println("Invalid option");
        }
    }

    public void deleteRecord(BufferedReader reader, String tableName) throws IOException {
        System.out.println("Input the primary key of the record.");
        System.out.print(">> ");
        String primaryKeyValue = reader.readLine();
        if(primaryKeyValue.trim().isEmpty()){
            System.out.println("You haven't entered any content.");
            return;
        }

        ArrayList<MetadataManager.AttributesInformation> attributesInformations;
        attributesInformations = metadataManager.getAttributes(tableName);
        String location = metadataManager.getMethod(tableName, "location");
        Integer length = Integer.parseInt(metadataManager.getMethod(tableName, "length"));
        String primaryKey = metadataManager.getMethod(tableName, "primary_key");

        RandomAccessFile raf = new RandomAccessFile(location, "rw");
        Integer start = 0;
        String pointer = null;
        Integer pointingPointer = null;
        while(true){
            Integer i=0;
            byte[] bytes = new byte[length * 3];
            raf.seek(start);
            raf.read(bytes);
            String block= new String(bytes, "UTF-8");
            if(block.trim().isEmpty()){
                System.out.println("No corresponding records exist.");
                return;
            }

            String[] subStringArray = block.split("(?<=\\G.{" + length + "})");
            for(String str: subStringArray){
                if(!str.trim().isEmpty()){
                    if((Pattern.matches("^[0-9]+$", str.trim()))){
                        pointer = ""+str;
                        pointingPointer = start + (i*length);
                    }
                    if(str.contains(primaryKeyValue)){
                        String string = str;
                        String checkPrimaryKey = "";
                        for(MetadataManager.AttributesInformation information: attributesInformations){
                            if(information.getAttribute().toString().equals(primaryKey)) {
                                if(checkPrimaryKey.substring(0, information.getLength().intValue()).trim().equals(primaryKeyValue)) {

                                    String afterDelete = block.substring(0, i*length)+pointer+block.substring((i+1)*length);
                                    String recordIndex = String.format("%" + length + "s", start + (i*length));
                                    raf.seek(start);
                                    raf.write(afterDelete.getBytes("UTF-8"));

                                    bytes = new byte[length * 3 +1];
                                    start = (length*3)*(pointingPointer/(length*3));
                                    raf.seek(start);
                                    raf.read(bytes);
                                    block= new String(bytes, "UTF-8");
                                    afterDelete = block.substring(0, pointingPointer-start)+recordIndex+block.substring(pointingPointer-start+length);
                                    raf.seek(start);
                                    raf.write(afterDelete.getBytes("UTF-8"));
                                    return;
                                }
                            }
                            else{
                                checkPrimaryKey = string.substring(information.getLength());
                                string = checkPrimaryKey;
                            }
                        }
                    }
                }
                i++;
            }
            start = start + (length*3);
        }
    }

    void partitioning(String tableName, String column) throws IOException {
        //임시 파일 생성
        for(int i=0; i<5; i++) {
            FileOutputStream fos = new FileOutputStream(System.getProperty("user.dir") + "\\tmp\\" + tableName + i + ".txt");
            fos.close();
        }
        //버퍼 bucket 생성 및 초기화
        List<String>[] bucket = new ArrayList[5];
        for(int i=0; i<5; i++){
            bucket[i] = new ArrayList<>();
        }

        ArrayList<MetadataManager.AttributesInformation> attributesInformations;
        attributesInformations = metadataManager.getAttributes(tableName);
        String location = metadataManager.getMethod(tableName, "location");
        Integer length = Integer.parseInt(metadataManager.getMethod(tableName, "length"));

        BufferedReader fr = new BufferedReader(new FileReader(location), length*3);

        char[] buffer = new char[length*3];
        while(true) {
            int charsRead = fr.read(buffer, 0, length*3);
            if(charsRead == -1)
                break;

            String block2 = new String(buffer, 0, charsRead);
            String[] subStringArray = block2.split("(?<=\\G.{" + length + "})");
            for(String string : subStringArray){
                if(!string.trim().isEmpty()) {
                    if (!(Pattern.matches("^[0-9\\s]+$", string))) {
                        String str = string;
                        String CheckColumn = "";
                        for(MetadataManager.AttributesInformation information: attributesInformations) {
                            if (information.getAttribute().toString().equals(column)) {
                                //column 파싱 및 해시 적용
                                String columnValue = CheckColumn.substring(0, information.getLength().intValue()).trim();
                                columnValue = Arrays.toString(columnValue.getBytes());
                                int sum = Arrays.stream(columnValue.substring(1, columnValue.length() - 1).split(","))
                                        .map(String::trim)
                                        .mapToInt(Integer::parseInt)
                                        .sum();
                                int hash = sum%5;

                                //해당 버퍼에 저장
                                bucket[hash].add(string);
                                //버퍼가 가득차면 디스크로 씀
                                if(bucket[hash].size() == 2){
                                    try {
                                        String toWrite = "";
                                        for (String elements: bucket[hash]){
                                            toWrite = toWrite+elements;
                                        }
                                        FileWriter writer = new FileWriter(System.getProperty("user.dir") + "\\tmp\\" + tableName + hash + ".txt", true);
                                        writer.write(toWrite);
                                        writer.close();
                                    } catch (IOException e) {
                                        System.out.println(e.getMessage());
                                    }
                                    bucket[hash].clear();
                                }

                                //다음 컬럼부터는 확인하지 않음
                                break;
                            }
                            else {
                                CheckColumn = str.substring(information.getLength());
                                str = CheckColumn;
                            }
                        }
                    }
                }
            }
        }
        //버킷에 있는 값들 디스크로 저장
        for(int i=0; i<5; i++){
            try {
                String toWrite = "";
                for (String elements: bucket[i]){
                    toWrite = toWrite+elements;
                }
                FileWriter writer = new FileWriter(System.getProperty("user.dir") + "\\tmp\\" + tableName + i + ".txt", true);
                writer.write(toWrite);
                writer.close();
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
    }



    public class BucketStructure{
        String columnValue;

        Integer index;

        public String getColumnValue() {
            return columnValue;
        }

        public void setColumnValue(String columnValue) {
            this.columnValue = columnValue;
        }

        public Integer getIndex() {
            return index;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }
    }

    public void join(String table1, String column1, String table2, String column2) throws IOException {
        ArrayList<MetadataManager.AttributesInformation> attributesInformations1;
        attributesInformations1 = metadataManager.getAttributes(table1);
        Integer length1 = Integer.parseInt(metadataManager.getMethod(table1, "length"));
        ArrayList<MetadataManager.AttributesInformation> attributesInformations2;
        attributesInformations2 = metadataManager.getAttributes(table2);
        Integer length2 = Integer.parseInt(metadataManager.getMethod(table2, "length"));

        //partitioning
        partitioning(table1, column1);
        partitioning(table2, column2);

        //join 짝 찾기
        List<BucketStructure>[] bucket = new ArrayList[3];
        for(int i=0; i<3; i++){
            bucket[i] = new ArrayList<>();
        }

        for(MetadataManager.AttributesInformation information: attributesInformations1){
            System.out.print("|"+String.format("%-"+information.getLength()+"s", information.getAttribute()));
        }
        for(MetadataManager.AttributesInformation information: attributesInformations2){
            System.out.print("|"+String.format("%-"+information.getLength()+"s", information.getAttribute()));
        }
        System.out.println("|");

        for(int i=0; i<5; i++){
            //build input, probe input 결정
            String location1 = System.getProperty("user.dir") + "\\tmp\\" + table1 + i + ".txt";
            Path path1 = Paths.get(location1);
            String location2 = System.getProperty("user.dir") + "\\tmp\\" + table2 + i + ".txt";
            Path path2 = Paths.get(location2);

            if(Files.size(path1) < Files.size(path2)) {
                //build input은 전부 메모리에 적재
                List<String> buildInput = new ArrayList<>();
                BufferedReader fr = new BufferedReader(new FileReader(location1), length1 * 2);

                char[] buffer = new char[length1 * 2];
                while (true) {
                    int charsRead = fr.read(buffer, 0, length1 * 2);
                    if (charsRead == -1)
                        break;

                    String block1 = new String(buffer, 0, charsRead);
                    for (int j = 0; j < block1.length(); j += length1) {
                        buildInput.add(block1.substring(j, j + length1));
                    }
                }
                fr.close();
                //hash index 구성
                for (int a=0; a< buildInput.size(); a++) {
                    //column parsing
                    String str = buildInput.get(a);
                    String CheckColumn = "";
                    for (MetadataManager.AttributesInformation information : attributesInformations1) {
                        if (information.getAttribute().toString().equals(column1)) {
                            //column 파싱 및 해시 적용
                            String columnValue = CheckColumn.substring(0, information.getLength().intValue()).trim();
                            //columnValue = Arrays.toString(columnValue.getBytes());
                            int hash = (columnValue.hashCode() % 3 + 3) % 3;

                            BucketStructure bucketStructure = new BucketStructure();
                            bucketStructure.setColumnValue(columnValue);
                            bucketStructure.setIndex(a);

                            bucket[hash].add(bucketStructure);
                        } else {
                            CheckColumn = str.substring(information.getLength());
                            str = CheckColumn;
                        }
                    }
                }


                fr = new BufferedReader(new FileReader(location2), length2 * 2);

                buffer = new char[length2 * 2];
                while (true) {
                    int charsRead = fr.read(buffer, 0, length2 * 2);
                    if (charsRead != -1) {
                        //probe input은 한 블록씩 가져와서 hash index에 동일한 내용이 있는지 확인
                        String block2 = new String(buffer, 0, charsRead);
                        for (int j = 0; j < block2.length(); j += length2) {
                            String probeInput = block2.substring(j, j + length2);

                            //column parsing
                            String str = probeInput;
                            String CheckColumn = "";
                            for (MetadataManager.AttributesInformation information : attributesInformations2) {
                                if (information.getAttribute().toString().equals(column2)) {
                                    //column 파싱 및 해시 적용
                                    String columnValue = CheckColumn.substring(0, information.getLength().intValue()).trim();
                                    //columnValue = Arrays.toString(columnValue.getBytes());
                                    int hash = (columnValue.hashCode() % 3 + 3) % 3;

                                    //해시 인덱스를 사용하여 동일한 값이 있는지 확인
                                    for (BucketStructure bucketStructure : bucket[hash]) {
                                        //동일한 값이 있으면 출력
                                        if (columnValue.equals(bucketStructure.getColumnValue())) {
                                            String buildinput = buildInput.get(bucketStructure.getIndex());
                                            for(MetadataManager.AttributesInformation information1: attributesInformations1){
                                                System.out.print("|" + buildinput.substring(0, information1.getLength()));
                                                buildinput = buildinput.substring(information1.getLength());
                                            }
                                            for(MetadataManager.AttributesInformation information2: attributesInformations2){
                                                System.out.print("|" + probeInput.substring(0, information2.getLength()));
                                                probeInput = probeInput.substring(information2.getLength());
                                            }
                                            System.out.println("|");
                                        }
                                    }
                                } else {
                                    CheckColumn = str.substring(information.getLength());
                                    str = CheckColumn;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                fr.close();
                for (int a = 0; a < 3; a++) {
                    bucket[a].clear();
                }
            } else {
                //table2를 build input으로 사용
                List<String> buildInput = new ArrayList<>();
                BufferedReader fr = new BufferedReader(new FileReader(location2), length2 * 2);

                char[] buffer = new char[length2 * 2];
                while (true) {
                    int charsRead = fr.read(buffer, 0, length2 * 2);
                    if (charsRead == -1)
                        break;

                    String block2 = new String(buffer, 0, charsRead);
                    for (int j = 0; j < block2.length(); j += length2) {
                        buildInput.add(block2.substring(j, j + length2));
                    }
                }
                fr.close();
                //hash index 구성
                for (int a=0; a< buildInput.size(); a++) {
                    //column parsing
                    String str = buildInput.get(a);
                    String CheckColumn = "";
                    for (MetadataManager.AttributesInformation information : attributesInformations2) {
                        if (information.getAttribute().toString().equals(column2)) {
                            //column 파싱 및 해시 적용
                            String columnValue = CheckColumn.substring(0, information.getLength().intValue()).trim();
                            //columnValue = Arrays.toString(columnValue.getBytes());
                            int hash = (columnValue.hashCode() % 3 + 3) % 3;

                            BucketStructure bucketStructure = new BucketStructure();
                            bucketStructure.setColumnValue(columnValue);
                            bucketStructure.setIndex(a);

                            bucket[hash].add(bucketStructure);
                        } else {
                            CheckColumn = str.substring(information.getLength());
                            str = CheckColumn;
                        }
                    }
                }


                fr = new BufferedReader(new FileReader(location1), length1 * 2);

                buffer = new char[length1 * 2];
                while (true) {
                    int charsRead = fr.read(buffer, 0, length1 * 2);
                    if (charsRead != -1) {
                        //probe input은 한 블록씩 가져와서 hash index에 동일한 내용이 있는지 확인
                        String block1 = new String(buffer, 0, charsRead);
                        for (int j = 0; j < block1.length(); j += length1) {
                            String probeInput = block1.substring(j, j + length1);

                            //column parsing
                            String str = probeInput;
                            String CheckColumn = "";
                            for (MetadataManager.AttributesInformation information : attributesInformations1) {
                                if (information.getAttribute().toString().equals(column1)) {
                                    //column 파싱 및 해시 적용
                                    String columnValue = CheckColumn.substring(0, information.getLength().intValue()).trim();
                                    //columnValue = Arrays.toString(columnValue.getBytes());
                                    int hash = (columnValue.hashCode() % 3 + 3) % 3;

                                    //해시 인덱스를 사용하여 동일한 값이 있는지 확인
                                    for (BucketStructure bucketStructure : bucket[hash]) {
                                        //동일한 값이 있으면 출력
                                        if (columnValue.equals(bucketStructure.getColumnValue())) {
                                            String buildinput = buildInput.get(bucketStructure.getIndex());
                                            for(MetadataManager.AttributesInformation information1: attributesInformations1){
                                                System.out.print("|" + probeInput.substring(0, information1.getLength()));
                                                probeInput = probeInput.substring(information1.getLength());
                                            }
                                            for(MetadataManager.AttributesInformation information2: attributesInformations2){
                                                System.out.print("|" + buildinput.substring(0, information2.getLength()));
                                                buildinput = buildinput.substring(information2.getLength());
                                            }
                                            System.out.println("|");
                                        }
                                    }
                                } else {
                                    CheckColumn = str.substring(information.getLength());
                                    str = CheckColumn;
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                fr.close();
                for (int a = 0; a < 3; a++) {
                    bucket[a].clear();
                }
            }
        }

        //임시파일 삭제
        File file = new File(System.getProperty("user.dir") + "\\tmp\\");
        if(file.exists()){
            if(file.isDirectory()){
                File[] files = file.listFiles();

                for(int i=0; i<files.length; i++){
                    files[i].delete();
                }
            }
        }
    }
}
