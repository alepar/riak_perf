package ru.alepar.perf;

public interface TpsGenerator {
    void start();

    interface Listener {
        void onSuccess();

        void onError(Exception e);
    }
}
