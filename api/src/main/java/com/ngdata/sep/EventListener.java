package com.ngdata.sep;

/**
 * Handles incoming Side-Effect Processor messages.
 */
public interface EventListener {

    /**
     * Process a message that has been delivered via the Side-Effect Processor (SEP).
     * <p>
     * If an exception is thrown while processing a message, the same message (along with any others that were batched
     * along with it) will be retried later by the SEP. For this reason, message handling should be idempotent.
     * 
     * @param row The row key of the record for which the event is being processed
     * @param payload Contains the event data
     */
    void processMessage(byte[] row, byte[] payload);
}
