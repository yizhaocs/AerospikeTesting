package com.adara.newcache.servlet;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.opinmind.ssc.cache.AerospikeService;
import org.apache.log4j.Logger;
import org.springframework.web.HttpRequestHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


/**
 * scp /Users/yzhao/IdeaProjects/AerospikeTesting/dist/aerospiketesting.war manager:/home/yzhao
 * ssh manager
 * scp aerospiketesting.war qa-inweb1:/home/yzhao
 * ssh qa-inweb1
 * sudo cp aerospiketesting.war /opt/apache-tomcat/webapps/
 * sudo /sbin/service tomcat restart
 * tail -f /opt/apache-tomcat/logs/catalina.out
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=write&type=string&start=0&end=2000&database=ao&table=set09062017"
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=read&type=string&start=0&end=2000&database=ao&table=set09062017"
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=delete&type=string&start=0&end=2000&database=ao&table=set09062017"
 *
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=write&type=integer&start=0&end=2000&database=ao&table=set09062017"
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=read&type=integer&start=0&end=2000&database=ao&table=set09062017"
 * curl "http://localhost:8080/aerospiketesting/aerospiketesting?mode=delete&type=integer&start=0&end=2000&database=ao&table=set09062017"
 *
 *
 */


/**
 * @author YI ZHAO
 */
public class AerospikeTestingServlet implements HttpRequestHandler {
    private static final Logger log = Logger.getLogger(AerospikeTestingServlet.class);

    private AerospikeService aerospikeService;
    static String columnName1 = "column1";
    static String columnName2 = "column2";
    static String columnName3 = "column3";

    public void handleRequest(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        System.out.println("[AerospikeTestingServlet.handleRequest]");

        String mode = req.getParameter("mode");
        int start =  Integer.valueOf(req.getParameter("start"));
        int end = Integer.valueOf(req.getParameter("end"));
        String type = req.getParameter("type");
        String database = req.getParameter("database");
        String table = req.getParameter("table");
        int count = 0;
        if(mode.equals("read")){
            long startTime = System.nanoTime();
            if(type.equals("string")){
                for(int i = start; i < end; i++){
                    Key row = new Key(database, table, String.valueOf(i));
                    aerospikeService.getRecord(null, row);
                    count++;
                }
            }else if(type.equals("integer")){
                for(int i = start; i < end; i++){
                    Key row = new Key(database, table, i);
                    aerospikeService.getRecord(null, row);
                    count++;
                }
            }

            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for read: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for read: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }else if(mode.equals("write")){
            long startTime = System.nanoTime();
            if(type.equals("string")) {
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, String.valueOf(i));
                    Bin bin1 = new Bin(columnName1, String.valueOf(i));
                    Bin bin2 = new Bin(columnName2, String.valueOf(i + 1));
                    Bin bin3 = new Bin(columnName3, String.valueOf(i + 2));
                    aerospikeService.putRecord(null, row, bin1, bin2, bin3);
                    count++;
                }
            }else if(type.equals("integer")){
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, i);
                    Bin bin1 = new Bin(columnName1, i);
                    Bin bin2 = new Bin(columnName2, i + 1);
                    Bin bin3 = new Bin(columnName3, i + 2);
                    aerospikeService.putRecord(null, row, bin1, bin2, bin3);
                    count++;
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }else if(mode.equals("deletecolumn")){
            long startTime = System.nanoTime();
            if(type.equals("string")) {
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, String.valueOf(i));
                    aerospikeService.deleteColumn(null, row, columnName1);
                    aerospikeService.deleteColumn(null, row, columnName2);
                    aerospikeService.deleteColumn(null, row, columnName3);
                    count++;
                }
            }else if(type.equals("integer")){
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, i);
                    aerospikeService.deleteColumn(null, row, columnName1);
                    aerospikeService.deleteColumn(null, row, columnName2);
                    aerospikeService.deleteColumn(null, row, columnName3);
                    count++;
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for columntonull: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for columntonull: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }else if(mode.equals("deleterow")){
            long startTime = System.nanoTime();
            if(type.equals("string")) {
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, String.valueOf(i));
                    aerospikeService.deleteRow(null,row);
                    count++;
                }
            }else if(type.equals("integer")){
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, i);
                    aerospikeService.deleteRow(null,row);
                    count++;
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for columntonull: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for columntonull: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }else if(mode.equals("deletecolumnandrow")){
            long startTime = System.nanoTime();
            if(type.equals("string")) {
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, String.valueOf(i));
                    aerospikeService.deleteColumn(null, row, columnName1);
                    aerospikeService.deleteColumn(null, row, columnName2);
                    aerospikeService.deleteColumn(null, row, columnName3);
                    aerospikeService.deleteRow(null,row);
                    count++;
                }
            }else if(type.equals("integer")){
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, i);
                    aerospikeService.deleteColumn(null, row, columnName1);
                    aerospikeService.deleteColumn(null, row, columnName2);
                    aerospikeService.deleteColumn(null, row, columnName3);
                    aerospikeService.deleteRow(null,row);
                    count++;
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for delete: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for delete: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }else if(mode.equals("AsyncWrite")){
            long startTime = System.nanoTime();
            if(type.equals("string")) {
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, String.valueOf(i));
                    Bin bin1 = new Bin(columnName1, String.valueOf(i));
                    Bin bin2 = new Bin(columnName2, String.valueOf(i + 1));
                    Bin bin3 = new Bin(columnName3, String.valueOf(i + 2));
                    aerospikeService.putRecordAsync(null, row, bin1, bin2, bin3);
                    count++;
                }
            }else if(type.equals("integer")){
                for (int i = start; i < end; i++) {
                    Key row = new Key(database, table, i);
                    Bin bin1 = new Bin(columnName1, i);
                    Bin bin2 = new Bin(columnName2, i + 1);
                    Bin bin3 = new Bin(columnName3, i + 2);
                    aerospikeService.putRecordAsync(null, row, bin1, bin2, bin3);
                    count++;
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime)/1000000;  //divide by 1000000 to get milliseconds.
            System.out.println("[AerospikeTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
            log.info("[AerospikeTestingServlet.handleRequest]: duration for write: total with " + duration + " milliseconds ,and per query:" + duration/(count) + " milliseconds,  count:" + count);
        }
    }


    public void init() throws ServletException {
        System.out.println("[AerospikeTestingServlet.init]");
    }

    public void destroy() throws ServletException {
        System.out.println("[AerospikeTestingServlet.destroy]");
    }

    public void setAerospikeService(AerospikeService aerospikeService) {
        this.aerospikeService = aerospikeService;
    }
}
