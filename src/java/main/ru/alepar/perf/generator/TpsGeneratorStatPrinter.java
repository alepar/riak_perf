package ru.alepar.perf.generator;

import ru.alepar.perf.stat.RunningWindowStat;
import ru.alepar.perf.stat.StatPerSec;

import java.io.PrintStream;

public class TpsGeneratorStatPrinter implements TpsGenerator.Listener {

    private final StatPerSec errorStat = new RunningWindowStat(100, 11);
    private final StatPerSec successStat = new RunningWindowStat(100, 11);

    public TpsGeneratorStatPrinter(PrintStream out) {
        new Thread(() -> {
            while(true) {
                try {
                    Thread.sleep(250);

                    out.print(String.format("tps: %5.1f ok, %5.1f error\r", successStat.avg(), errorStat.avg()));
                } catch (Exception ignored) {
                }
            }
        }).start();
    }

    @Override
    public void onSuccess() {
        successStat.add(1);
    }

    @Override
    public void onError(Exception e) {
        errorStat.add(1);
    }

}
