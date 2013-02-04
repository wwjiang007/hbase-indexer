package com.ngdata.sep;

/**
 * Handles incoming Side-Effect Processor messages.
 */
public interface EventListener {

    /**
     * Process an event that has been delivered via the Side-Effect Processor (SEP).
     * <p>
     * If an exception is thrown while processing a message, the same message (along with any others that were batched
     * along with it) will be retried later by the SEP. For this reason, message handling should be idempotent.
     * 
     * @param event contains data involved in the HBase update
     */
    void processEvent(SepEvent event);
}
