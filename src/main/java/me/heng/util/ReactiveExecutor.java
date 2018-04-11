package me.heng.util;

import com.alibaba.fastjson.JSON;

import java.util.concurrent.CompletableFuture;

import static me.heng.util.Supports.format;


/**
 * AUTHOR: wangdi
 * DATE: 11/04/2018
 * TIME: 3:01 PM
 */
public interface ReactiveExecutor {


    <T, R> void sync(Fx<T, R, Throwable> fx);

    <T, R> void sync(Fx<T, R, Throwable> fx, Ex<T, R> ex);

    <T, R> void async(Fx<T, R, Throwable> fx);

    <T, R> void async(Fx<T, R, Throwable> fx, Ex<T, R> ex);

    /**
     * 按照 delays 延迟顺序,按序执行fx,直到返回true/object!=null等
     *
     * @param <T>
     * @param <R>
     * @param delays
     * @param fx
     */
    <T, R> void schedule(int delays, Fx<T, R, Throwable> fx);

    <T, R> void schedule(int delays, Fx<T, R, Throwable> fx, Ex<T, R> ex);

    /**
     * 重试times次,每次间隔 interval
     *
     * @param <T>
     * @param <R>
     * @param times
     * @param interval
     * @param fx
     */
    <T, R> void retry(int times, int interval, Fx<T, R, Throwable> fx);

    <T, R> void retry(int times, int interval, Fx<T, R, Throwable> fx, Ex<T, R> ex);

    /**
     * 执行 fx, 超时millis未成功则返回
     *
     * @param fx
     * @param millis
     * @param <T>
     * @param <R>
     */
    <T, R> void timeout(Fx<T, R, Throwable> fx, int millis);

    <T, R> void timeout(Fx<T, R, Throwable> fx, int millis, Ex<T, R> ex);

    /**
     * 提交任务定义
     *
     * @return
     */
    <R, I> CompletableFuture<R> submit(I input);

    <R, I> CompletableFuture<R> submit(I input, Ex<Object, Object> ex);

    void shutdown();

    FlowEnv flowEnv();

    /**
     * 可抛出异常的函数,阻塞函数
     */
    interface Fx<I, O, E extends Throwable> {
        O apply(I input) throws E;
    }

    interface BiFx<I1, I2, O, E extends Throwable> {
        O apply(I1 input, I2 input2) throws E;
    }

    interface TriFx<I1, I2, I3, O, E extends Throwable> {
        O apply(I1 input, I2 input2, I3 input3) throws E;
    }

    interface Cx<I, E extends Throwable> {
        void apply(I input) throws E;
    }

    interface Sx<O, E extends Throwable> {
        O apply() throws E;
    }

    interface BiCx<I1, I2, E extends Throwable> {
        void apply(I1 input, I2 input2) throws E;
    }

    interface TriCx<I1, I2, I3, E extends Throwable> {
        void apply(I1 input, I2 input2, I3 input3) throws E;
    }

    /**
     * 串联函数
     *
     * @param f1
     * @param f2
     * @return
     */
    static <A, B, C> Fx<A, C, Throwable> join(Fx<A, B, ? extends Throwable> f1,
                                              Fx<B, C, ? extends Throwable> f2) {
        return (A input) -> {
            B result = f1.apply(input);
            return f2.apply(result);
        };
    }

    /**
     * 异常处理,如果异常可以处理,恢复 返回 O,否则抛出异常,只抛出 unchecked 异常
     *
     */
    interface Ex<I, O> {
        O apply(Throwable ex, I input);
    }

    /**
     * ReactiveExecutorExecution 接口相关操作异常
     */
    class OperatorException extends RuntimeException {
        private String message;

        public OperatorException(String message) {
            super(message);
            this.message = message;
        }

        public OperatorException(String fmt, Object... args) {
            this(format(fmt, args));
        }

        @Override
        public String toString() {
            return "OperatorException{" + this.message + "}";
        }

        @Override
        public String getMessage() {
            return this.toString();
        }
    }

    /**
     * retry 操作符
     */
    class RetryException extends OperatorException {

        int initDelay;
        int times;
        int intervals;
        Object input;
        Object last;
        private String errorSum;

        public RetryException(String message) {
            super(message);
        }

        public RetryException(String errorSum, int initDelay, int times, int intervals,
                              Object input, Object last) {
            super("RetryException");
            this.errorSum = errorSum;
            this.initDelay = initDelay;
            this.times = times;
            this.intervals = intervals;
            this.input = input;
            this.last = last;
        }

        @Override
        public String toString() {
            return errorSum + "RetryException{" + "initDelay=" + initDelay + ", times=" + times
                    + ", intervals=" + intervals + ", input=" + JSON.toJSONString(input) + '}';
        }
    }

    /**
     * timeout 操作符
     */
    class TimeoutException extends OperatorException {
        int timeout;

        public TimeoutException(String message) {
            super(message);
        }

        public TimeoutException(String fmt, Object... args) {
            super(fmt, args);
        }

        @Override
        public String toString() {
            return "TimeoutException{" + "timeout=" + timeout + '}';
        }
    }


    /**
     * Ex 处理异常
     */
    class HandlerException extends OperatorException {
        public HandlerException(String message) {
            super(message);
        }

        public HandlerException(String fmt, Object... args) {
            super(fmt, args);
        }
    }


    /**
     * 流程执行过程的变量问题
     */
    interface FlowEnv {
        <T> T get(String key);

        <T> void set(String key, T val);

        boolean remove(String key);

        void destroy();
    }

}
