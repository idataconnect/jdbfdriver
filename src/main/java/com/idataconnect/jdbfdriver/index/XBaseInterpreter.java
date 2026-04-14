package com.idataconnect.jdbfdriver.index;

import com.idataconnect.jdbfdriver.DBF;

/**
 * Interface for a pluggable xBase expression interpreter.
 */
public interface XBaseInterpreter {
    
    /**
     * Evaluates an xBase expression.
     * @param expression the expression string
     * @param dbf the current DBF instance (for context)
     * @return the result of the evaluation
     * @throws Exception if evaluation fails
     */
    Object evaluate(String expression, DBF dbf) throws Exception;
    
    /**
     * Gets the priority of this interpreter. Lower numbers are higher priority.
     * @return the priority
     */
    default int getPriority() {
        return 100;
    }
}
