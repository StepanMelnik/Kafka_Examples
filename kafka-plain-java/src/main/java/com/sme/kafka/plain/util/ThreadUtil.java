package com.sme.kafka.plain.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Useful utilities to work with threads in unit tests.
 */
public final class ThreadUtil
{
    // private
    private ThreadUtil()
    {
    }

    /**
     * Sleep the current thread in milliseconds.
     * 
     * @param timeout The timeout to sleep;
     * @param consumer String consumer;
     * @param message The given message to consume.
     */
    public static void sleepInMilliSeconds(long timeout, Consumer<String> consumer, String message)
    {
        try
        {
            TimeUnit.MILLISECONDS.sleep(timeout);
        }
        catch (InterruptedException e)
        {
            consumer.accept(message);
        }
    }

    /**
     * Sleep the current thread in seconds.
     * 
     * @param timeout The timeout to sleep;
     * @param consumer String consumer;
     * @param message The given message to consume.
     */
    public static void sleepInSeconds(long timeout, Consumer<String> consumer, String message)
    {
        try
        {
            TimeUnit.SECONDS.sleep(timeout);
        }
        catch (InterruptedException e)
        {
            consumer.accept(message);
        }
    }
}
