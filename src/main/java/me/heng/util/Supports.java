package me.heng.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.PrintStream;
import java.util.*;

/**
 * AUTHOR: wangdi
 * DATE: 11/04/2018
 * TIME: 2:59 PM
 */
public class Supports {

    public static PrintStream stdout = System.out;

    /**
     * 控制台打印，格式化字符串
     */
    public static void println(String fmt, Object... objs) {
        stdout.println(format(fmt, objs));
    }

    /**
     * 格式化字符串
     *
     * @param fmt String
     * @param objs Object...
     * @return String
     */
    public static String format(String fmt, Object... objs) {
        String line = fmt;
        if (objs != null && objs.length > 0) {
            line = String.format(fmt, objs);
        }
        return line;
    }

    public static <K> boolean isEmpty(K[] ks) {
        return ks == null || ks.length == 0;
    }

    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isEmpty(Iterable<?> iterable) {
        if (iterable instanceof Collection) {
            return isEmpty((Collection) iterable);
        }
        return iterable == null || iterable.iterator().hasNext();
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

//    public static boolean isEmpty(String string) {
//        return string == null || string.isEmpty();
//    }
//
//    public static boolean isNotEmpty(String string) {
//        return string != null && !string.isEmpty();
//    }

    public static boolean isNotEmpty(Collection<?> collection) {
        return !isEmpty(collection);
    }

    public static <T> boolean isNotEmpty(T... objects) {
        return !isEmpty(objects);
    }

    public static boolean isNotEmpty(Iterable<?> objects) {
        return !isEmpty(objects);
    }

    public static boolean isNotEmpty(Map<?, ?> map) {
        return !isEmpty(map);
    }

    /**
     * 判断字符串是否空白字符串
     *
     * @return
     */
//    public static boolean isBlank(String string) {
//        return isEmpty(string) || isEmpty(string.trim());
//    }

    public static boolean isNotBlank(String string) {
        return isNotEmpty(string) && isNotEmpty(string.trim());
    }

    /**
     * 返回一个空的HashMap
     *
     * @return HashMap
     */
    public static <K, V> Map<K, V> map() {
        return Maps.newHashMap();
    }

    public static <K, V> Map<K, V> map(K k1, V v1) {
        Map<K, V> map = Maps.newHashMap();
        map.put(k1, v1);
        return map;
    }

    public static <K, V> Map<K, V> map(K k1, V v1, K k2, V v2) {
        Map<K, V> map = map(k1, v1);
        map.put(k2, v2);
        return map;
    }

    public static <V> Map<V, V> map(V... kvs) {
        Map<V, V> map = Maps.newHashMap();
        if (isEmpty(kvs))
            return map;
        int end = kvs.length - 1;
        for (int idx = 0; idx < end && idx + 1 <= end; idx += 2) {
            map.put(kvs[idx], kvs[idx + 1]);
        }
        return map;
    }

    public static <K> List<K> list(K... eles) {
        return Arrays.asList(eles);
    }

    public static <T> List<T> list(Iterable<? extends T> iterable) {
        if (iterable == null) {
            return Collections.emptyList();
        }
        if (iterable instanceof List) {
            return ((List) iterable);
        }
        return Lists.newArrayList(iterable);
    }

    public static <T> List<T> list(Iterator<? extends T> iterator) {
        if (iterator == null || !iterator.hasNext()) {
            return Collections.emptyList();
        }
        return Lists.newArrayList(iterator);
    }

    public static <K> Set<K> set(K... eles) {
        return Sets.newHashSet(eles);
    }

    public static <K> Set<K> set(Iterable<? extends K> iterable) {
        return Sets.newHashSet(iterable);
    }

    /**
     * 判断左右字符串是否匹配 忽略大小写
     *
     * @return boolean
     */
//    public static boolean like(String left, String right) {
//        if (left == null || right == null)
//            return left == right;
//        return eq(left.toLowerCase(), right.toLowerCase());
//    }

    /**
     * 针对枚举类型
     *
     * @param left
     * @param right
     * @return boolean
     */
//    public static boolean like(Object left, Object right) {
//        if (left == null || right == null)
//            return left == right;
//        Object l = left, r = right;
//        if (left instanceof Enum) {
//            l = ((Enum) left).name();
//        }
//        if (right instanceof Enum) {
//            r = ((Enum) right).name();
//        }
//        return eq(l.toString().toLowerCase(), r.toString().toLowerCase());
//    }

//    public static boolean eq(Object left, Object right) {
//        return Objects.equals(left, right);
//    }

    /**
     * 返回第一个对象，或者Null
     *
     * @param ts ts
     * @return 返回第一个对象，或者Null
     */
    public static <T> T firstOrNull(T... ts) {
        if (ts == null || ts.length == 0)
            return null;
        return ts[0];
    }

    public static <T> T firstOrNull(Collection<T> list) {
        return firstOrDefault(list, null);
    }

    public static <K, V> V firstOrNull(Map<K, V> map) {
        return firstOrDefault(map, null);
    }

    public static <T> T firstOrDefault(Collection<? extends T> list, T defValue) {
        if (list == null || list.size() == 0)
            return defValue;
        return list.iterator().next();
    }

    public static <K, V> V firstOrDefault(Map<K, V> map, V defValue) {
        if (map == null || map.size() == 0)
            return defValue;
        return map.values().iterator().next();
    }

    /**
     * 判断所有对象是否为null
     *
     * @param objects objects
     * @return 当存在一个对象为null时 true
     */
    public static boolean anyNull(Object... objects) {
        if (isEmpty(objects))
            return true;
        for (Object object : objects) {
            if (object == null)
                return true;
        }
        return false;
    }

    /**
     * 判断所有对象不存在null
     *
     * @param objects objects
     * @return 只有当所有对象均不为null时 true
     */
    public static boolean notNull(Object... objects) {
        return !anyNull(objects);
    }


    public static <V> boolean existNotNull(Iterable<V> iterable) {
        if (iterable == null)
            return false;
        for (V v : iterable) {
            if (v != null)
                return true;
        }
        return false;
    }

    /**
     * 检查每个string isEmpty
     *
     * @param objs String
     * @return
     */
//    public static boolean anyEmpty(String... objs) {
//        for (String obj : objs) {
//            if (isEmpty(obj))
//                return true;
//        }
//        return false;
//    }

    public static String join(Collection<String> objs, String delimiters) {
        return Joiner.on(delimiters).skipNulls().join(objs);
    }

    public static String join(String split, String... ids) {
        return Joiner.on(split).join(ids);
    }

    public static List<String> split(String target, String regex) {
        if (Supports.isNotEmpty(target)) {
            return Arrays.asList(target.split(regex));
        }
        return Collections.emptyList();
    }

}
