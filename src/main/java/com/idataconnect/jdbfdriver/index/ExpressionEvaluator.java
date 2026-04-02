package com.idataconnect.jdbfdriver.index;

/**
 * Interface for evaluating xBase expressions.
 */
public interface ExpressionEvaluator {
    
    /**
     * Evaluates the given expression.
     * @param expression the expression to evaluate
     * @param context optional context (e.g. DBF instance)
     * @return the result of the evaluation
     * @throws Exception if evaluation fails
     */
    Object evaluate(String expression, Object context) throws Exception;
}
