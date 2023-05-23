package ldbc.finbench.acid.transactions;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class TransactionThread<T, R> implements Callable<R> {

    final long transactionId;
    final Function<T, R> fn;
    final T params;

    public TransactionThread(long transactionId, Function<T, R> fn, T t) {
        this.transactionId = transactionId;
        this.fn = fn;
        this.params = t;
    }

    @Override
    public R call() throws Exception {
        return fn.apply(params);
    }

    public long getTransactionId() {
        return transactionId;
    }

}
