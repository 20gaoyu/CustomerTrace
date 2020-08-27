package com.boco.customer.etl.O_RE_ST_XDR_PS_S1U_DNS;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import com.boco.customer.etl.O_RE_ST_XDR_PS_S1U_HTTP.OReStXdrPsS1uHttpMapper;
import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.HbaseConfigUtils;
import com.boco.customer.hbase.table.Spliter;

public class OReStXdrPsS1uDnsDriver {


    static final String NAME = "S1uDnsDetail";
    // 日志
    static final Log log = LogFactory.getLog(OReStXdrPsS1uDnsDriver.class);
    // 配置文件
    private static Properties pro = HbaseConfigUtils.getConfigs();
    private static String if_index;

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	//long l1 = System.currentTimeMillis();
	// 输入参数必须大于等于2 
	if (args.length < 3) {
	    log.error("error args size,must 3....");
	    System.exit(1);
	}
	// 截取时间长度
	String day = checkDay(args[0]);
	String outdir = args[2];
	// 获取参数
	String quorum = pro.getProperty("hbase.zookeeper.quorum");
	String clientPort = pro.getProperty("hbase.zookeeper.property.clientPort");
	String table = pro.getProperty("dns.name");
	
	//用来查询imsi的需要增加索引表，rowkey的值为主表的rowkey
	String index = pro.getProperty("dns.index");
		
	String family = pro.getProperty("dns.family");
	String column = pro.getProperty("dns.column");
	String keyLen = pro.getProperty("dns.key.length");
	String kerberos_count = pro.getProperty("kerberos_count");
	String if_kerberos = pro.getProperty("if_kerberos");
	if_index = pro.getProperty("dns.if.index");
	String province_name = pro.getProperty("province.name");
	String mainTable = table.replace("$DAY$", day);
	//用来查询imsi的需要增加索引表，rowkey的值为主表的rowkey
	String indexTable = index.replace("$DAY$", day);
	// 设置hbase参数
	Configuration conf = HBaseConfiguration.create();
	conf.set("hbase.zookeeper.quorum", quorum);
	conf.set("hbase.zookeeper.property.clientPort", clientPort);
	conf.set("table.name", mainTable);
	conf.set("table.family", family);
	conf.set("table.column", column);
	conf.set("table.key.length", keyLen);
	//索引表
	conf.set("table.index", indexTable);
	conf.set("table.if.index", if_index);
	conf.set("province.name", province_name);
	// 512M
	conf.set("mapred.min.split.size", "536870912");
	conf.set("mapred.max.split.size", "536870913");
	if("true".equals(if_kerberos))
	{
	log.info("进行kerberos认证，账号："+kerberos_count);
	 //设置安全验证方式为kerberos
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
    conf.set("keytab.file", "/home/boco/boco.keytab");
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hbase.security.authentication", "kerberos");
    //设置hbase master及hbase regionserver的安全标识，这两个值可以在hbase-site.xml中找到
    conf.set("hbase.master.kerberos.principal", "hbase/_HOST@HADOOP.COM");
    conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@HADOOP.COM");
    //conf.set("mapred.job.queue.name", "queue_ywgz");
    conf.set("mapreduce.job.queuename", "boco");
    //使用设置的用户登陆
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(kerberos_count, "/home/boco/boco.keytab");
	}
	// 检测表是否存在，不在创建，加上索引表
	checkTable(conf, mainTable,indexTable, day);

	Job job = CreateSubmitTableJob(conf, day, args[1],outdir);
	Connection connection = ConnectionFactory.createConnection(conf);
    TableName tab = TableName.valueOf(mainTable);
    HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tab), connection.getRegionLocator(tab));

	boolean succ = job.waitForCompletion(true);
	connection.close();
	long total = job.getCounters().findCounter(HbaseCounter.ALL_RECORD).getValue();
	long len_err = job.getCounters().findCounter(HbaseCounter.LEN_ERR).getValue();
	long keyword_err = job.getCounters().findCounter(HbaseCounter.KEYWORD_ERR).getValue();
	long insert_err = job.getCounters().findCounter(HbaseCounter.INSERT_ERR).getValue();
	log.info("total:" + total + ",len_err:" + len_err + ",keyword_err:" + keyword_err + ",insert_err" + insert_err);
	System.exit(succ ? 0 : 1);
    }

    /**
     * 创建job
     * 
     * @param conf
     * @param day
     * @param inputDir
     * @return
     * @throws IOException
     */
    private static Job CreateSubmitTableJob(Configuration conf, String day, String inputDir,String outDir) throws IOException {
	// 创建job实例
	Job job = new Job(conf, NAME + "_" + day);
	job.setJarByClass(OReStXdrPsS1uDnsMapper.class);

	Path path = new Path(inputDir);
	ArrayList<Path> filevals = new ArrayList<Path>();
	FileSystem fs = path.getFileSystem(conf);
	FileStatus[] fss = fs.listStatus(path);
	filevals = buildFs(conf, filevals, fss);
	log.info("file size:" + filevals.size());
	for (Path p : filevals) {
	    FileInputFormat.addInputPath(job, p);
	}
	job.setInputFormatClass(TextInputFormat.class);
	job.setMapperClass(OReStXdrPsS1uDnsMapper.class);
	job.setNumReduceTasks(135);
	job.setOutputFormatClass(NullOutputFormat.class);
	 
	job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    FileOutputFormat.setOutputPath(job, new Path(outDir));
     
	TableMapReduceUtil.addDependencyJars(job);
	return job;
    }

    /**
     * 输入路径
     * 
     * @param conf
     * @param filevals
     * @param fss
     * @return
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private static ArrayList<Path> buildFs(Configuration conf, ArrayList<Path> filevals, FileStatus[] fss) throws IOException {
	for (FileStatus status : fss) {
	    Path p = status.getPath();
	    if (status.isDir()) {// 是目录
		FileSystem fs = p.getFileSystem(conf);
		filevals = buildFs(conf, filevals, fs.listStatus(p));
	    } else {// 不是目录
		filevals.add(p);
	    }
	}
	return filevals;
    }

    /**
     * 获取八位日期格式YYYYMMDD
     * 
     * @param d
     * @return
     */
    public static String checkDay(String d) {
	if (null == d || "".equals(d)) {
	    log.error("input time " + d + "error ,must:yyyymmddhh24mi");
	    System.exit(1);
	}
	return d.substring(0, 8);
    }

    /**
     * 检查hbase的表是否存在，不存在创建
     * 
     * @param conf
     * @param mainTable
     * @param day
     */
    private static void checkTable(Configuration conf, String mainTable,String indexTable, String day) {
	try {
	    @SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
	    if (!admin.tableExists(mainTable)) {
		log.info("table not create,now create table:" + mainTable);
		// 创建表并进行预分区
		Spliter.createDnsTable(conf,mainTable);
	    }
	    if ("true".equals(if_index)&&!admin.tableExists(indexTable)) {
			log.info("table not create,now create table:" + indexTable);
			// 创建表并进行预分区
			Spliter.createIndexTable(indexTable);
		    }
	} catch (ParseException e) {
	    e.printStackTrace();
	} catch (IOException e) {
	    e.printStackTrace();
	}

    }
}
