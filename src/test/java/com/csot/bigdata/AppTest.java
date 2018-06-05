package com.csot.bigdata; /**
 * App.java
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.CellUtil.cloneValue;

public class AppTest {
    public static void main(String[] args) throws IOException {
        String ReadFilePath = "C:\\bigdata\\study\\hadoopDemo\\HbaseQuery\\hash.csv";
        String SaveFilePath = "C:\\bigdata\\study\\hadoopDemo\\HbaseQuery\\result.csv";
        String tableName = "pdm.m_fdc_param_summary_wide_id";
        ArrayList glasslist = readDataFile(ReadFilePath);
        ArrayList key = new ArrayList();
        String glass = "";
        for (int i = 0; i < glasslist.size(); i++) {
            glass = glass + glasslist.get(i) + " ";
        }
        String eqp = "CBGAP120";
        // 建立solr连接
        String SOLR_URL = "172.22.6.120:2181,172.22.6.121:2181,172.22.6.122:2181/solr";
        CloudSolrServer solrServer = new CloudSolrServer(SOLR_URL);
        solrServer.setDefaultCollection("m_fdc_param_summary_wide_id");
        solrServer.setZkClientTimeout(20000);
        solrServer.setZkConnectTimeout(10000);
        solrServer.connect();
        SolrQuery query = new SolrQuery();
        //下面设置solr查询参数
        //query.set("q", "*:*");// 参数q  查询所有
        query.setStart(0);
        query.setRows(1000000000);
        query.setQuery("m_fdc_param_summary_wide_glass_id:(" + glass + ")");
        //query.addFilterQuery("m_fdc_param_summary_wide_eqp_id:"+eqp);
        //获取查询结果
        QueryResponse response = null;
        try {
            response = solrServer.query(query);
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
        //两种结果获取：得到文档集合或者实体对象

        //查询得到文档的集合
        SolrDocumentList solrDocumentList = response.getResults();
        System.out.println("通过文档集合获取查询的结果");
        System.out.println("查询结果的总数量：" + solrDocumentList.getNumFound());
        //遍历列表
        for (SolrDocument doc : solrDocumentList) {
            System.out.println("glass_id:" + doc.get("m_fdc_param_summary_wide_glass_id") + "   id:" + doc.get("id"));
            key.add(doc.get("id"));
        }


        // 建立hbase连接
        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "10.108.240.103,10.108.240.105,10.108.240.107");
        conf.set("hbase.zookeeper.quorum", "172.22.6.120,172.22.6.121,172.22.6.122");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = null;
        Admin admin = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            if (admin != null) {
                System.out.println("connect hbase successfully");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Save Result ==========================");
        // 查询单笔数据
//        try {
//            String GettableName = "pdm.m_edc_param_wide_id";
//            Table table = conn.getTable(TableName.valueOf(GettableName));
//            Get get = new Get(Bytes.toBytes("-1000000043_1513059268"));
//            get.addColumn(Bytes.toBytes("param"), Bytes.toBytes("param_list"));
//            Result result = table.get(get);
//            for (Cell cell : result.rawCells()) {
//                System.out.println(
//                        "Rowkey : " + Bytes.toString(result.getRow()) +
//                                "   Familiy:Quilifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
//                                "   Value : " + Bytes.toString(CellUtil.cloneValue(cell))
//                );
//            }
//
//            table.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // 查询多笔数据

        /* 方式一 */
        try {
            List<String> r = new ArrayList();
            long begin0 = System.currentTimeMillis();
            SaveQueryResultBatch(key, conn, tableName, SaveFilePath);
            long end0 = System.currentTimeMillis();
            System.out.println("BufferedOutputStream执行耗时:" + (end0 - begin0) + "豪秒");
            //          CallR();
//                   System.out.println("GET VALUE ==========================");
//                    r = qurryTableTestBatch(glasslist, conn,tableName);
//                    for (int j = 0; j < r.size(); j++) {
//                        System.out.println("每个实体对象值为" + r.get(j));
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//方式二
//        try {
//            List<String> r2 = new ArrayList();
//            r2 = qurryTableTestBatch(glasslist, conn,tableName);
//            for (int j = 0; j < r2.size(); j++) {
//                System.out.println("每个实体对象值为" + r2.get(j));
//
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        // 关闭连接
        try {
            if (admin != null) {
                admin.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static List<String> qurryTableTestBatch(List<String> rowkeyList, Connection conn, String tableName) throws IOException {
        List<Get> getList = new ArrayList();
        List<String> list = new ArrayList();
        Table table = conn.getTable(TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList) {//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        for (Result result : results) {//对返回的结果集进行操作
            for (Cell kv : result.rawCells()) {
                printKeyValye((KeyValue) kv);
                String value = Bytes.toString(cloneValue(kv));
                list.add(value);
            }
        }
        return list;
    }

    public static void SaveQueryResultBatch(List<String> rowkeyList, Connection conn, String tableName, String SaveFilePath) throws IOException {
        List<Get> getList = new ArrayList();
        ArrayList<String> resultlist = new ArrayList();
        Table table = conn.getTable(TableName.valueOf(tableName));// 获取表
        for (String rowkey : rowkeyList) {//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        resultlist.add("glass_id,param_name,param_value" + "\n");
        for (Result result : results) {//对返回的结果集进行操作
            for (Cell kv : result.rawCells()) {
                resultlist.add(Bytes.toString(kv.getRow()) + "," + Bytes.toString(kv.getQualifier()) + "," + Bytes.toString(kv.getValue()).replace(",", "") + "\n");
            }
        }
        //將結果写入文件
        BufferedWriter bw = new BufferedWriter(new FileWriter(SaveFilePath));
        //遍历集合
        for (String s : resultlist) {
            //写数据
            bw.write(s);
            bw.flush();
        }
        //释放资源
        bw.close();
    }

//    public static List<String> qurryTableTest(List<String> rowkeyList, Connection conn, String tableName) throws IOException {
//        Table table = conn.getTable(TableName.valueOf(tableName));// 获取表
//        List<String> list = new ArrayList();
//        for (String rowkey : rowkeyList) {
//            Get get = new Get(Bytes.toBytes(rowkey));
//            Result result = table.get(get);
//            for (Cell kv : result.rawCells()) {
//                String value = Bytes.toString(CellUtil.cloneValue(kv));
//                list.add(value);
//            }
//        }
//        return list;
//    }

    public static void printKeyValye(KeyValue kv) {
        System.out.println(Bytes.toString(kv.getRow()) + "\t" + Bytes.toString(kv.getFamily()) + "\t" + Bytes.toString(kv.getQualifier()) + "\t" + Bytes.toString(kv.getValue()) + "\t" + kv.getTimestamp());
    }


    public static void printCell(Cell cell) {
        System.out.println(Bytes.toString(cell.getRow()) + "\t" + Bytes.toString(cell.getFamily()) + "\t" + Bytes.toString(cell.getQualifier()) + "\t" + Bytes.toString(cell.getValue()) + "\t" + cell.getTimestamp());
    }

    public static ArrayList<String> readDataFile(String filePath) {
        File file = new File(filePath);
        ArrayList<String> dataArray = new ArrayList<String>();

        try {
            BufferedReader in = new BufferedReader(new FileReader(file));
            String str;
            while ((str = in.readLine()) != null) {
                dataArray.add(str);
            }
            in.close();
        } catch (IOException e) {
            e.getStackTrace();
        }
        return dataArray;
    }

    public static void CallR() {
        //方式一
//        try {
//            Process pro = Runtime.getRuntime().exec("cmd /c"+" "+ "D:/IdeaWorkSpace/HbaseQuery/start.bat"); //添加要进行的命令
//            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream())); //虽然cmd命令可以直接输出，但是通过IO流技术可以保证对数据进行一个缓冲。
//            String msg = null;
//            while ((msg = br.readLine()) != null) {
//                System.out.println(msg);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        //方式二
//        ProcessBuilder pb = new ProcessBuilder();
//        pb.redirectErrorStream(true);
//        try {
//           // Process p = pb.command("D:/IdeaWorkSpace/HbaseQuery/start.bat").start();
//           Process p = pb.command("Rscript","D:/IdeaWorkSpace/HbaseQuery/convert.r").start();
//            //Process p = pb.command("ipconfig").start();
//            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(),"UTF-8"));
//            String s = "";
//            while((s=br.readLine())!= null){
//                System.out.println(s);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        //方式三
        Runtime run = Runtime.getRuntime();// 返回与当前 Java 应用程序相关的运行时对象
        try {
            String cmds = "Rscript D:\\IdeaWorkSpace\\HbaseQuery\\convert.r"; // 注意：对字符串中路径\进行转义
            Process p = run.exec(cmds);// 启动另一个进程来执行命令
        } catch (Exception e) {
            e.printStackTrace();
        }
        //方式四
//        Rengine re = new Rengine(new String[] { "--vanilla" }, false, null);
//        if(!re.waitForR()) {
//            System.out.println("无法载入R.");
//            return;
//        }
//        re.eval("source(\"D:/IdeaWorkSpace/HbaseQuery/convert.r\")");
//        System.out.println(re.eval("CF()"));
//        re.end();

    }
}


