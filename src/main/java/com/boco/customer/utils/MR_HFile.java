package com.boco.customer.utils;


import com.boco.customer.hbase.HbaseConfigUtils;
import com.boco.customer.hbase.table.Spliter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by zhang on 2016/12/19.
 */
public class MR_HFile extends Configured implements Tool {

    private static String family = "f";
    private static String column = "c";
    private static Properties pro = HbaseConfigUtils.getConfigs();

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new MR_HFile(), args);
        System.exit(ret);
    }


    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        //输入参数必须大于等于2
        if (args.length != 4) {
            System.exit(1);
        }
        // 截取时间长度
        String in = args[0];
        String NAME = args[2];
        String out = args[1];
        String dataTime = args[3];

        // 获取参数
        String quorum = pro.getProperty("hbase.zookeeper.quorum");
        String clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
        String table = pro.getProperty(NAME + ".name");
        String keyLen = pro.getProperty(NAME + ".key.length");
        String mainTable = table.replace("$DAY$", dataTime);

        // 设置hbase参数
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("table.name", mainTable);
        conf.set("table.key.length", keyLen);

        //设置安全验证方式为kerberos
        System.setProperty("java.security.krb5.conf", "/home/ywgz/krb5.conf");
        conf.set("keytab.file", "/home/ywgz/ywgz.keytab");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        //设置hbase master及hbase regionserver的安全标识，这两个值可以在hbase-site.xml中找到
        conf.set("hbase.master.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN");
        conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@CHINATELECOM.CN");
        //conf.set("mapred.job.queue.name", "queue_ywgz");
        conf.set("mapreduce.job.queuename", "queue_ywgz_hbase");
        //使用设置的用户登陆
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("ywgz/chinatelecom@CHINATELECOM.CN", "/home/ywgz/ywgz.keytab");


        //创建表
        // 检测表是否存在，不在创建
       // Spliter.checkTable(conf, mainTable);
        // 创建job
        Job job = new Job(conf, mainTable);
        job.setJarByClass(MR_HFile.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(HFileMapper.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tab = TableName.valueOf(mainTable);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tab), connection.getRegionLocator(tab));

        boolean succ = job.waitForCompletion(true);
        connection.close();
        return succ ? 0 : 1;
    }


    public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] xdrdata = value.toString().split("\\^", -1);
            byte[] rowkey = xdrdata[0].getBytes();
            String v = xdrdata[1];
            ImmutableBytesWritable k = new ImmutableBytesWritable(rowkey);
            KeyValue kvProtocol = new KeyValue(rowkey, family.getBytes(), column.getBytes(), v.getBytes());
            context.write(k, kvProtocol);
        }
    }
}
