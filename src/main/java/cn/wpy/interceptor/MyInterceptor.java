package cn.wpy.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Auther：xydsr
 * @Date： 2023/6/20 19:22
 * @Desc：
 */
public class MyInterceptor implements Interceptor {
    // 初始化
    @Override
    public void initialize() {

    }

    // 处理一个event

    /**
     *  需求：
     *  log='{
     * "host":"www.baidu.com",
     * "user_id":"13755569427",
     * "items":[
     *     {
     *         "item_type":"eat",
     *         "active_time":156234
     *     },
     *     {
     *         "item_type":"car",
     *         "active_time":156233
     *     }
     *  ]
     * }'
     *
     * 需要转化为：
     * [{"active_time":156234,"user_id":"13755569427","item_type":"eat","host":"www.baidu.com"},
     * {"active_time":156233,"user_id":"13755569427","item_type":"car","host":"www.baidu.com"}]
     */

    @Override
    public Event intercept(Event event) {
        // 将event中的数据内容获取出来
        byte[] body = event.getBody();

        ArrayList<HashMap<String, String>> hashMapArrayList = new ArrayList<HashMap<String, String>>();
        // 将event中的内容，转换为string类型，并指定字符集
        String logs = new String(body, StandardCharsets.UTF_8);
        // logs 这个字符串起始就是一个json字符串，json字符串可以变为json对象
        // 通过json对象可以快速获取里面的数据
        // json工具： fastjson  jackson  gson 等

        JSONObject jsonObject = JSON.parseObject(logs);
        // json字符串变为了json对象
        String user_id = jsonObject.getString("user_id");
        String host = jsonObject.getString("host");

        JSONArray items = jsonObject.getJSONArray("items");
        for (Object o:items) {
            /**
             *         {
             *             "item_type":"eat",
             *             "active_time":156234
             *         }
             */
            String s = o.toString();
            JSONObject jsonObj = JSON.parseObject(s);
            String item_type = jsonObj.getString("item_type");
            String active_time = jsonObj.getString("active_time");

            HashMap<String, String> map = new HashMap<>();
            map.put("item_type",item_type);
            map.put("active_time",active_time);
            map.put("user_id",user_id);
            map.put("host",host);

            hashMapArrayList.add(map);

        }
        // 循环结束，list集合数据算是成功了，但是需要翻入到evenet的body中
        String jsonString = JSON.toJSONString(hashMapArrayList);
        event.setBody(jsonString.getBytes(StandardCharsets.UTF_8));
        return event;
    }

    // 处理一堆event
    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList<Event> events = new ArrayList<>();
        for (Event oldEvent:list) {
            Event newEvent = intercept(oldEvent);
            events.add(newEvent);
        }
        return events;
    }

    @Override
    public void close() {

    }

    // 需要写一个内部类，用于new 这个新的interceptor
    public static class BuilderEvent implements Builder{

        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
