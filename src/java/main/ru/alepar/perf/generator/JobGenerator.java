package ru.alepar.perf.generator;

public interface JobGenerator {
    Runnable next();
}
