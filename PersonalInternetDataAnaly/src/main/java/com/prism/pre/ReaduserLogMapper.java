package com.prism.pre;
/**   
 * @Description: 这是一个基于hadoop2.0的mr。
 * 处理手机测操作日志Mapper。 
 * @author 肖发腾 xiaofateng@gmail.com   
 * @date 2015年4月18日 下午10:26:53 
 * @version V1.0   
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//日志格式 tab分割
//userId(用户的id) action(用户的行为，即是打开社交软件还是打开浏览器等) actionUrl(如果行为是打开浏览器，打开的url地址) 
//actionApp(如果是打开app，app的名字) actionMsg(如果是打开社交软件，聊天内容) actionTime(行为时间)

public class ReaduserLogMapper extends Mapper<LongWritable, Text, Text, Text> {
@Override
protected void map(LongWritable key, Text value,
		Context context)
		throws IOException, InterruptedException {
	String[] values=value.toString().split(",");
	
	//过滤垃圾数据，一般情况下，很有必要过滤垃圾数据。
	if (values.length!=6) {
		return;
	}
	//取各个字段
	String userId=values[0];
	String action=values[1];
	String actionUrl=values[2];
	String actionApp=values[3];
	String actionMsg=values[4];
	String actionTime=values[5];
	
	//输出
	context.write(new Text(userId), new Text(action));
}
}
