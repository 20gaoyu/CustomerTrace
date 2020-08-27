package com.boco.customer.etl.OReStXdrPsLteCustrack;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

import com.boco.customer.hbase.HbaseConfigUtils;
import com.boco.customer.hbase.HbaseCounter;
import com.boco.customer.hbase.table.Spliter;

public class OReStXdrPsLteCustrackDriver {

	static final String NAME = "Custrack";
	// 日志
	static final Log log = LogFactory.getLog(OReStXdrPsLteCustrackDriver.class);
	// 配置文件
	private static Properties pro = HbaseConfigUtils.getConfigs();

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		long l1 = System.currentTimeMillis();
		// 输入参数必须大于等于4
		if (args.length < 4) {
			log.error("error args size,must 4....");
			// arg0 日期
			// arg1 http输入目录
			// arg2 dns输入目录
			// arg3 mme输入目录
			System.exit(1);
		}
		// 截取时间长度
		String day = checkDay(args[0]);

		// 获取参数
		String quorum = pro.getProperty("hbase.zookeeper.quorum");
		String clientPort = pro
				.getProperty("hbase.zookeeper.property.clientPort");
		String table = pro.getProperty("custrack.name");
		String family = pro.getProperty("custrack.family");
		String column = pro.getProperty("custrack.column");
		String keyLen = pro.getProperty("custrack.key.length");
		String mainTable = table.replace("$DAY$", day);
		String province_name = pro.getProperty("province.name");
		String kerberos_count = pro.getProperty("kerberos_count");
		String if_kerberos = pro.getProperty("if_kerberos");
		// 设置hbase参数
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("hbase.zookeeper.property.clientPort", clientPort);
		conf.set("table.name", mainTable);
		conf.set("table.family", family);
		conf.set("table.column", column);
		conf.set("table.key.length", keyLen);

		// 512M
		conf.set("mapred.min.split.size", "536870912");
		conf.set("mapred.max.split.size", "536870913");
		conf.set("province.name", province_name);
		conf.set("kerberos_count", kerberos_count);
		conf.set("if_kerberos", if_kerberos);
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
		// 检测表是否存在，不在创建
		checkTable(conf, mainTable, day);

		// 创建job
		// Job job = CreateSubmitTableJob(conf, day, args[1]);
		Job job = new Job(conf, NAME + "_" + day);
		job.setJarByClass(OReStXdrPsLteCustrackDriver.class);
		
		
		// maper的输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.addDependencyJars(job);
		
		log.info("开始进入http的map");
		// http mapper
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, OReStXdrPsLteHttp1Mapper.class);
		log.info("开始进入dns的map");
		// dns mapper
		MultipleInputs.addInputPath(job, new Path(args[2]),
				TextInputFormat.class, OReStXdrPsLteDnsMapper.class);
		// mme mapper
		MultipleInputs.addInputPath(job, new Path(args[3]),
				TextInputFormat.class, OReStXdrPsLteMmeMapper.class);

		// reducer
		job.setReducerClass(OReStXdrPsLteCustrackReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		
		
		
		boolean succ = job.waitForCompletion(true);

		long total = job.getCounters().findCounter(HbaseCounter.ALL_RECORD)
				.getValue();
		long error = job.getCounters().findCounter(HbaseCounter.LEN_ERR)
				.getValue();
		long numerror = job.getCounters().findCounter(HbaseCounter.KEYWORD_ERR)
				.getValue();
		log.info("total:" + total + ",error:" + error + ",number err:"
				+ numerror + ",cost:" + (System.currentTimeMillis() - l1));
		System.exit(succ ? 0 : 1);

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
	private static void checkTable(Configuration conf, String mainTable,
			String day) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (!admin.tableExists(mainTable)) {
				log.info("table not create,now create table:" + mainTable);
				// 创建表并进行预分区
				Spliter.createCustrackTable(conf,mainTable);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
