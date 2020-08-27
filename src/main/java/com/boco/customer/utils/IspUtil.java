package com.boco.customer.utils;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;

import com.boco.customer.utils.RedisUtils;


public class IspUtil {
    public static Map<String,String> map = new HashMap<String,String>();
    public static Set<Long> set  =new TreeSet<Long>();
    public static List<Long> list  =new ArrayList<Long>();
    
//  初始化
    static{
    Configuration conf = new Configuration();
    String valueHttpCau="1,2,3,4";
	try {
		map = HdfsToDim.getDimInfo(conf, "/user/boco/dim/DIM_ISP_IP_REDUCE/", "0", valueHttpCau);
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	for(String key:map.keySet()){
	    set.add(Long.valueOf(key));
	}
	list.addAll(set);
    }
    
    /**
     * @param server_id end_ip转换为long型后的值
     * @return
     */
    public static String getIspByEndIP(long server_id){
	Long key=searchNear(server_id);
	return map.get(key+"");
    }
    
    public static long searchNear(Long realIp){
	int index_small=0;
	int index_big=list.size()-1;
	int index_center=(index_small+index_big)/2;
	
//	特例
	if(realIp<=list.get(0)){
	    return list.get(0);
	}
	
	while(index_small<index_center){
	   long center_val=list.get(index_center);
	    if(center_val==realIp){
		return center_val;
	    }else if (center_val > realIp) {
		index_big = index_center;
		index_center = (index_small + index_big) / 2;
	    } else {
		index_small = index_center;
		index_center = (index_small + index_big) / 2;
	    }
	}
	return list.get(index_center+1);
    }
}
