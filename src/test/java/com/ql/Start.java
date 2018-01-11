package com.ql;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

public class Start {

    public static void main(String[] args) throws Exception {
        String abc = "id,value,dateTime,quality,error,pointId,\n" +
                "1,0.0,2016-12-22 16:46:17,2,0,33779,\n" +
                "2,0.0,2016-12-22 17:06:58,0,0,33779,\n" +
                "3,0.0,2016-12-22 17:07:03,0,0,33779,\n" +
                "4,267.0,2016-12-23 07:50:06,0,0,33779,";

        System.out.println(abc.replaceAll("\"",""));

        String[] arr = abc.trim().split(",");
        System.out.println("======"+arr.length);
        int j = 1;   //值
        int k = 2;   //时间
        int l = 5;  //编码



        for (int i = 0; i < arr.length; i++) {
            // System.out.println(item);
            if (j >= arr.length) {
                return;
            }

            if (j != 1 && k!=2 && l!=5)  {
                String row = arr[l] + "-" + "QL" + "-" + arr[k];
                System.out.println(row + "====" + arr[j]);
               // context.write(new Text(row), new DoubleWritable(Integer.valueOf(arr[j])));
            }
            j += 6;
            k += 6;
            l += 6;
        }
    }
}
