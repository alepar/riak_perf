package ru.alepar.perf.generator;

public interface TpsGenerator {
    void start();

    interface Listener {
        void onSuccess();

        void onError(Exception e);
    }
}
