/**
 * LLS工具，类CSV式数据结构
 * 
 * @class LLSUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.quickutil.platform.SafeCharsetUtil.ObjectType;

public class LLSUtil {

    /**
     * List<Object>转LLS
     * 
     * @param list-对象数组
     * @return
     */
    public static List<List<Object>> objectsToLLS(List<Object> list) {
        if (list == null)
            return null;
        if (list.size() == 0)
            return null;
        Object object = list.get(0);
        if (SafeCharsetUtil.getObjectType(object) != ObjectType.other)
            return null;
        List<List<Object>> LLS = new LinkedList<List<Object>>();
        List<Object> headList = new LinkedList<Object>();
        for (Field field : object.getClass().getFields()) {
            headList.add(field.getName());
        }
        LLS.add(headList);
        for (Object obj : list) {
            List<Object> lineList = new LinkedList<Object>();
            for (Field field : obj.getClass().getFields()) {
                try {
                    lineList.add(field.get(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            LLS.add(lineList);
        }
        return LLS;
    }

    /**
     * List<Map>转LLS
     * 
     * @param list-哈希表数组
     * @return
     */
    public static List<List<Object>> mapsToLLS(List<Map<String, Object>> list) {
        if (list == null)
            return null;
        if (list.size() == 0)
            return null;
        List<List<Object>> LLS = new LinkedList<List<Object>>();
        List<Object> headList = new LinkedList<Object>();
        for (String key : list.get(0).keySet()) {
            headList.add(key);
        }
        LLS.add(headList);
        for (Map<String, Object> map : list) {
            List<Object> lineList = new LinkedList<Object>();
            for (int i = 0; i < headList.size(); i++) {
                lineList.add(map.get(headList.get(i).toString()));
            }
            LLS.add(lineList);
        }
        return LLS;
    }

    /**
     * LLS转List<Map>
     * 
     * @param LLS-LLS结构体
     * @return
     */
    public static List<Map<String, Object>> llsToListMap(List<List<Object>> LLS) {
        if (LLS == null)
            return null;
        if (LLS.size() < 2)
            return null;
        List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
        List<Object> headList = LLS.get(0);
        for (int i = 1; i < LLS.size(); i++) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int j = 0; j < headList.size(); j++) {
                map.put(headList.get(j).toString(), LLS.get(i).get(j));
            }
            list.add(map);
        }
        return list;
    }
}
