package me.heng.support;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.Uninterruptibles;
import me.heng.util.ReactiveExecutor;
import me.heng.util.ReactiveExecutor.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static me.heng.util.Supports.*;

/**
 * AUTHOR: wangdi
 * DATE: 11/04/2018
 * TIME: 2:55 PM
 */
public class ReactiveSupport {
    final static TimeUnit unit = TimeUnit.MILLISECONDS;

    public static <T> Ex<T, Boolean> falseExHandler() {
        return (ex, input) -> false;
    }

    public static <T> Ex<T, Boolean> trueExHandler() {
        return (ex, input) -> true;
    }

    public static <T, V> Function<T, V> always(V val) {
        return i -> val;
    }

    public static <T> Function<T, Boolean> alwaysTrue() {
        return (T i) -> true;
    }

    /**
     * List<Future> -> Future<List>
     *
     * notice CompletableFuture#allof的限制,若任意一个future异常,则返回异常
     *
     * @param futures
     * @return
     */
    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
        return CompletableFuture.allOf(cfs).thenApply(
                v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    public static <T> CompletableFuture<T> completedExceptionally(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    public static <I, O> CompletableFuture<List<Pair<O, Throwable>>> watch(Collection<I> inputs,
                                                                           Function<I, CompletableFuture<O>> fn) {

        List<CompletableFuture<O>> futures = map(inputs, input -> {
            CompletableFuture<I> f = CompletableFuture.completedFuture(input);
            return f.thenCompose(fn);
        });

        CompletableFuture<List<Pair<O, Throwable>>> future = watch(futures);
        return future;
    }

    /**
     * 同futureOf的区别在于有返回值有input信息
     *
     * @param inputs
     * @param fx
     * @param executor
     * @return
     */
    public static <I, O> CompletableFuture<List<Triple<I, O, Throwable>>> watchFx(
            Collection<I> inputs, Fx<I, O, Throwable> fx, Executor executor) {
        CompletableFuture<List<Pair<Triple<I, O, Throwable>, Throwable>>> futures =
                futureOf(inputs, (I in) -> {
                    try {
                        O out = fx.apply(in);
                        return Triple.of(in, out, null);
                    } catch (Throwable ex) {
                        return Triple.of(in, null, ex);
                    }
                } , executor);
        /**
         * 由于异常被catch, 不会抛出异常
         */
        CompletableFuture<List<Triple<I, O, Throwable>>> future =
                futures.thenApply(list->list.stream().map(Pair::getLeft).collect(Collectors.toList()));
//                futures.thenApply(list -> map(list, Pair::getLeft));
        return future;
    }

    public static <A, I, O> CompletableFuture<List<Triple<I, O, Throwable>>> watchFx(
            CompletableFuture<A> future, Collection<I> inputs, Fx<I, O, Throwable> fx,
            Executor executor) {
        CompletableFuture<List<Triple<I, O, Throwable>>> retFuture = new CompletableFuture<>();
        future.whenComplete((r, ex) -> {
            CompletableFuture<List<Triple<I, O, Throwable>>> futures =
                    watchFx(inputs, fx, executor);
            stare(retFuture, futures);
        });
        return retFuture;
    }

    /**
     * 监视 future列表,全部完成/抛出异常,则完成
     *
     * @param futures
     * @param <V>
     * @return
     */
    public static <V> CompletableFuture<List<Pair<V, Throwable>>> watch(
            Collection<CompletableFuture<V>> futures) {
        AtomicInteger latch = new AtomicInteger(futures.size());
        CompletableFuture<List<Pair<V, Throwable>>> future = new CompletableFuture<>();
        // List<Pair<V, Throwable>> list = new ArrayList<>(futures.size());
        /**
         * 并发是魔鬼, 注意ArrayList非thread safe!!!!!!!!
         */
        List<Pair<V, Throwable>> list =
                Collections.synchronizedList(new ArrayList<>(futures.size()));
        futures.forEach(f -> {
            f.whenComplete((v, ex) -> {
                list.add(Pair.of(v, ex));
                if (latch.decrementAndGet() <= 0) {
                    // Supports.println("list size:%d:%d", futures.size(), list.size());
                    future.complete(list);
                }
            });
        });
        return future;
    }

    /**
     *
     * @param future CompletableFuture
     * @param inputs 输入集合
     * @param fx 执行
     * @param executor 线程池
     * @return
     */
    public static <A, B, C> CompletableFuture<List<Triple<B, C, Throwable>>> watch(
            CompletableFuture<A> future, Collection<B> inputs,
            BiFx<A, B, C, ? extends Throwable> fx, Executor executor) {
        List<CompletableFuture<Triple<B, C, Throwable>>> futures = map(inputs, input -> {
            return future.thenApplyAsync(a -> {
                try {
                    C c = fx.apply(a, input);
                    return Triple.of(input, c, null);
                } catch (Throwable ex) {
                    return Triple.of(input, null, ex);
                }
            } , executor);
        });
        /**
         * futures 会是全部异常,若部分fx异常,也会算是正常返回(异常信息记录在triple中),所有可以使用sequence
         */
        return sequence(futures);
    }

    /**
     * 回调方式,该串行化(书写方式)
     *
     * @param future
     * @param fx
     * @param executor
     * @return
     */
    public static <A, B> CompletableFuture<B> wire(CompletableFuture<A> future,
                                                   Fx<A, B, Throwable> fx, Executor executor) {
        CompletableFuture<B> fb = new CompletableFuture<>();
        future.whenComplete((a, ex) -> {
            if (ex == null) {
                executor.execute(() -> {
                    try {
                        B result = fx.apply(a);
                        fb.complete(result);
                    } catch (Throwable throwable) {
                        fb.completeExceptionally(throwable);
                    }
                });
            } else
                fb.completeExceptionally(ex);
        });
        return fb;
    }

    /**
     * CompletableFuture.supplyAsync不可用,不会处理异常 CompletableFuture.completeExceptionally
     *
     * @return
     */
    public static <I, O> CompletableFuture<O> futureOf(I input,
                                                       Fx<? super I, ? extends O, ? extends Throwable> fx, Executor executor) {
        CompletableFuture<O> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                O result = fx.apply(input);
                future.complete(result);
            } catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    public static <I, O> CompletableFuture<List<Pair<O, Throwable>>> futureOf(Collection<I> inputs,
                                                                              Fx<? super I, ? extends O, ? extends Throwable> fx, Executor executor) {
        List<CompletableFuture<O>> futures = map(inputs, input -> futureOf(input, fx, executor));
        return watch(futures);
    }

    public static <O> CompletableFuture<O> futureOf(Sx<O, ? extends Throwable> sx,
                                                    Executor executor) {
        CompletableFuture<O> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(sx.apply());
            } catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    /**
     * 捕获异常, fx处理异常
     *
     * @param future
     */
    public static <O> CompletableFuture<O> catchFx(CompletableFuture<O> future,
                                                   Fx<Throwable, O, Throwable> fx) {
        CompletableFuture<O> f = new CompletableFuture<>();
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                f.complete(result);
            } else {
                Throwable throwable = unwrap(ex);
                try {
                    O fallback = fx.apply(throwable);
                    f.complete(fallback);
                } catch (Throwable error) {
                    f.completeExceptionally(error);
                }
            }
        });
        return f;
    }

    public static <O, E extends Throwable> CompletableFuture<O> catchFx(CompletableFuture<O> future,
                                                                        Class<E> clz, Fx<E, O, Throwable> fx) {
        return catchFx(future, (error) -> {
            if (clz.isAssignableFrom(error.getClass())) {
                return fx.apply((E) error);
            }
            throw error;
        });
    }

    /**
     * 展开CompletableFuture V-> Pair<V,Throwable>
     *
     * @param future
     */
    public static <V> CompletableFuture<Pair<V, Throwable>> unfold(CompletableFuture<V> future) {
        CompletableFuture<Pair<V, Throwable>> f = new CompletableFuture<>();
        future.whenComplete((v, ex) -> {
            if (ex == null) {
                f.complete(Pair.of(v, null));
            } else
                f.complete(Pair.of(null, ex));
        });
        return f;
    }

    public static <K, V> CompletableFuture<Triple<K, V, Throwable>> unfoldWithKey(K key,
                                                                                  CompletableFuture<V> future) {
        CompletableFuture<Triple<K, V, Throwable>> f = new CompletableFuture<>();
        future.whenComplete((v, ex) -> {
            if (ex == null) {
                f.complete(Triple.of(key, v, null));
            } else
                f.complete(Triple.of(key, null, ex));
        });
        return f;
    }

    public static <I, O> CompletableFuture<O> unpack(
            CompletableFuture<Triple<I, O, Throwable>> future) {
        CompletableFuture<O> f = new CompletableFuture<>();
        future.whenComplete((triple, ex) -> {
            if (ex == null) {
                if (triple.getRight() == null) {
                    f.complete(triple.getMiddle());
                } else
                    f.completeExceptionally(triple.getRight());
            } else
                f.completeExceptionally(ex);
        });
        return f;
    }

    /**
     * 取值操作
     *
     * @param future
     * @param fn
     * @param <I>
     * @param <O>
     * @return
     */
    public static <I, O> CompletableFuture<O> valueOf(CompletableFuture<I> future,
                                                      Function<I, O> fn) {
        return future.thenApply(fn);
    }

    /**
     * CompletableFuture 很多操作符会包装 Throwable eg. future.completeExceptionally
     *
     * @param ex
     * @return
     */
    public static Throwable unwrap(Throwable ex) {
        if (ex == null)
            return null;
        if (ex instanceof CompletionException && ex.getCause() != null) {
            return unwrap(ex.getCause());
        }
        return ex;
    }

    /**
     * 超时,如果超过timeout, future仍未结束,就异常结束
     *
     * @param future
     * @param timeout
     * @param scheduler
     */
    public static <V> CompletableFuture<V> timeout(CompletableFuture<V> future, int timeout,
                                                   ScheduledExecutorService scheduler) {
        scheduler.schedule(() -> {
            if (!future.isCancelled() && !future.isDone()) {
                synchronized (future) {
                    if (!future.isCancelled() && !future.isDone()) {
                        future.completeExceptionally(new ReactiveExecutor.TimeoutException("timeout:%d", timeout));
                    }
                }

            }
        } , timeout, unit);
        return future;
    }

    /**
     * 定时任务\调度相关的功能
     */
    /**
     *
     * @param times 整个运行的次数
     * @param initDelay 初始延迟
     * @param intervals 间隔
     * @param input 输入
     * @param fx fx
     * @param ex fx的异常处理函数, 返回false表示终止retry, 否则继续重试
     * @param es
     * @param errorSum 错误原因总结
     * @return
     */
    public static <I, T> CompletableFuture<T> retry(int times, int initDelay, int intervals,
                                                    I input, Fx<I, T, ? extends Throwable> fx, Ex<I, Boolean> ex,
                                                    ScheduledExecutorService es, String errorSum) {
        AtomicInteger count = new AtomicInteger(0);
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable runnable = () -> {
            if (count.incrementAndGet() <= times) {
                T val;
                try {
                    val = fx.apply(input);
                    if (val != null && (val instanceof Boolean && (Boolean) val == true
                            || !(val instanceof Boolean)))
                        future.complete(val);
                } catch (Throwable throwable) {
                    /**
                     * 返回 false,即终止重试
                     */
                    if (!ex.apply(throwable, input)) {
                        future.completeExceptionally(throwable);
                    }
                }
            } else {
                RetryException exception =
                        new RetryException(errorSum, initDelay, times, intervals, input, null);
                future.completeExceptionally(exception);
            }
            /**
             * 检查是否跳出重试, 通过抛出异常的方式,终止运行
             */
            if (future.isDone()) {
                throw new RuntimeException();
            }
        };
        es.scheduleAtFixedRate(runnable, initDelay, intervals, unit);
        return future;
    }

    public static <I, T> CompletableFuture<T> retry(int times, int intervals, I input,
                                                    Fx<I, T, ? extends Throwable> fx, Ex<I, Boolean> ex, ScheduledExecutorService es, String errorSum) {
        return retry(times, 0, intervals, input, fx, ex, es, errorSum);
    }

    /**
     * 延迟重试
     *
     * @param future
     * @param times
     * @param intervals
     * @param fx
     * @param ex
     * @param es
     * @return
     */
    public static <I, T> CompletableFuture<T> retry(CompletableFuture<I> future, int times,
                                                    int intervals, Fx<I, T, ? extends Throwable> fx, Ex<I, Boolean> ex,
                                                    ScheduledExecutorService es, String errorSum) {
        return retry(future, 0, times, intervals, fx, ex, es, errorSum);
    }

    public static <I, T> CompletableFuture<T> retry(CompletableFuture<I> future, int initDelay,
                                                    int times, int intervals, Fx<I, T, ? extends Throwable> fx, Ex<I, Boolean> ex,
                                                    ScheduledExecutorService es, String errorSum) {
        CompletableFuture<T> ft = new CompletableFuture<>();
        future.whenComplete((input, error) -> {
            if (error == null) {
                CompletableFuture<T> retryFuture =
                        retry(times, initDelay, intervals, input, fx, ex, es, errorSum);
                stare(ft, retryFuture);
            } else
                ft.completeExceptionally(error);
        });
        return ft;
    }

    public static <I, T> CompletableFuture<T> delay(int delay, I input, Fx<I, T, Throwable> fx,
                                                    ScheduledExecutorService executor) {
        CompletableFuture<T> ft = new CompletableFuture<>();
        executor.schedule(() -> {
            T result = null;
            try {
                result = fx.apply(input);
                ft.complete(result);
            } catch (Throwable throwable) {
                ft.completeExceptionally(throwable);
            }
        } , delay, unit);
        return ft;
    }

    /**
     * 延时执行
     *
     * @param future
     * @param delay
     * @param fx
     * @param es
     * @return
     */
    public static <I, T> CompletableFuture<T> delay(CompletableFuture<I> future, int delay,
                                                    Fx<I, T, Throwable> fx, ScheduledExecutorService es) {
        CompletableFuture<T> fd = new CompletableFuture<>();
        future.whenComplete((input, ex) -> {
            if (ex == null) {
                stare(fd, delay(delay, input, fx, es));
            } else
                fd.completeExceptionally(ex);
        });
        return fd;
    }

    /**
     * 紧盯, a的值盯着b的值变化
     *
     * @param futureA
     * @param futureB
     */
    public static <A> void stare(CompletableFuture<A> futureA, CompletableFuture<A> futureB) {
        futureB.whenComplete((b, ex) -> {
            if (ex == null) {
                futureA.complete(b);
            } else
                futureA.completeExceptionally(ex);
        });
    }

    /**
     * 监视A, 并使用 fx
     *
     * @param futureA
     * @param fx
     * @return
     */
    public static <A, B> CompletableFuture<B> stareFx(CompletableFuture<A> futureA,
                                                      Fx<A, CompletableFuture<B>, Throwable> fx) {
        CompletableFuture<B> future = new CompletableFuture<>();
        futureA.whenComplete((a, ex) -> {
            if (ex != null) {
                future.completeExceptionally(ex);
            } else
                try {
                    CompletableFuture<B> futureB = fx.apply(a);
                    stare(future, futureB);
                } catch (Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
        });
        return future;
    }

    public static <A, B> CompletableFuture<B> stare(CompletableFuture<A> futureA,
                                                    Function<A, B> fn) {
        CompletableFuture<B> future = futureA.thenApply(fn);
        return future;
    }

    /**
     * 监视futureA, futureB, 并使用fn对a,b处理
     *
     * @param futureA
     * @param futureB
     * @param fn
     * @return
     */
    public static <A, B, C> CompletableFuture<C> stare(CompletableFuture<A> futureA,
                                                       CompletableFuture<B> futureB, BiFunction<A, B, C> fn) {
        CompletableFuture<C> future = futureA.thenCombine(futureB, fn);
        return future;
    }

    public static <I, O> Fx<I, O, Throwable> fn2fx(Function<I, O> fn) {
        return input -> fn.apply(input);
    }

    public static void main(String... args) throws Exception {
        Fx<Void, String, RuntimeException> fx = v -> "xxxxx";
        String result = fx.apply(null);
        println(result);

        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        CompletableFuture<String> f4 = new CompletableFuture<>();

        new Thread(() -> {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            f1.complete("hello world");
            f2.completeExceptionally(new RuntimeException("test"));
        }).start();

        new Thread(() -> {
            f3.complete("侬好");
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            f4.completeExceptionally(new RuntimeException("test"));
        }).start();

        Collection<CompletableFuture<String>> futres = list(f1, f2, f3, f4);

        CompletableFuture<List<Pair<String, Throwable>>> f5 = watch((List) futres);

        // List<Pair<String, Throwable>> pairs = f5.get(30, TimeUnit.SECONDS);

        f5.whenComplete((pairs, ex) -> {
            for (Pair<String, Throwable> pair : pairs) {
                Thread myself = Thread.currentThread();
                StackTraceElement se = myself.getStackTrace()[0];
                println(JSON.toJSONString(se));
                println(pair.getLeft());
                println(String.valueOf(pair.getRight()));
            }
        });

        CompletableFuture<String> f6 = new CompletableFuture<>();
        f6.complete("xxxxxx");
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        f6.whenComplete((r, x) -> {
            println(r);
        });
        f6.whenComplete((r, x) -> {
            println(r);
        });
    }
}
