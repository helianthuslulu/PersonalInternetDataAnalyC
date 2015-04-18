package com.prism.pre;
/**   
 * @Description: 这是一个基于hadoop2.0的mr。
 * 这个mr主要是 讲手机测操作日志进行处理。 
 * @author 肖发腾 xiaofateng@gmail.com   
 * @date 2015年4月18日 下午10:26:53 
 * @version V1.0   
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;


public class ReadPhoneLogMain  {
	
public static void main(String[] args) throws Exception {

    JobConf conf = new JobConf();
    conf.set("hadoop.tmp.dir", "/home/hadoop/hadooptmp"); 
    conf.set("dfs.permissions","false");
    conf.set("mapred.job.tracker", "192.168.1.105:9001");
    conf.set("mapred.jar", "D:/hadoopjar/prism.jar"); 
    
    //这个属性，是将输出的记录，以逗号分割，默认是tab分割。
    conf.set("mapred.textoutputformat.separator", ",");

    //创建job，与hadoop1.0版本的创建方式不同。
    Job job =Job.getInstance();
    job.setJarByClass(ReadPhoneLogMain.class);
    job.setJobName("ReadPhoneLogMain");
    
    //设置一个mapper
    job.setMapperClass(ReaduserLogMapper.class);
    //设置reducer的个数为0，如果明确不使用reducer，务必将个数设为0
    //原因见:http://www.xiaofateng.com/?p=549
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(conf, new Path(
    		"hdfs://192.168.1.105:9000/user/hadoop/input/tianchi_mobile_recommend_train_user.csv"));
    FileOutputFormat.setOutputPath(conf, new Path(
    		"hdfs://192.168.1.105:9000/user/hadoop/output/tianchi_userCategory"));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
