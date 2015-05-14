package me.bliss.kafka.web.home.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.utils, v 0.1 3/4/15
 *          Exp $
 */
public class MapUtils {

    public String joinKeyToString(Map<String,Object> map,String join){
        final Set<String> keySet = map.keySet();
        final Iterator<String> iterator = keySet.iterator();
        final StringBuffer result = new StringBuffer();
        while (iterator.hasNext()){
            result.append(iterator.next()).append(join);
        }
        return result.toString().substring(0,result.toString().length()-1);
    }

    public String convertMapToString(Map<String,Object> map,String join,String delimiter){
        final Set<Map.Entry<String, Object>> entries = map.entrySet();
        final Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        final StringBuffer result = new StringBuffer();
        while (iterator.hasNext()){
            final Map.Entry<String, Object> entry = iterator.next();
            result.append(entry.getKey()).append(join).append(entry.getValue()).append(delimiter);
        }
        return result.toString().substring(0,result.toString().lastIndexOf(delimiter));
    }
}
