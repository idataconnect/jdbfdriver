package com.idataconnect.jdbfdriver.index;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * An expression evaluator that uses ServiceLoader to find the best available XBaseInterpreter.
 */
public class JSR233ExpressionEvaluator implements ExpressionEvaluator {

    private final List<XBaseInterpreter> interpreters = new ArrayList<>();

    public JSR233ExpressionEvaluator() {
        // Load interpreters from classpath
        ServiceLoader<XBaseInterpreter> loader = ServiceLoader.load(XBaseInterpreter.class);
        for (XBaseInterpreter interpreter : loader) {
            interpreters.add(interpreter);
        }
        
        // Always add the simple fallback
        interpreters.add(new SimpleXBaseInterpreter());
        
        // Sort by priority (lower is higher)
        interpreters.sort(Comparator.comparingInt(XBaseInterpreter::getPriority));
    }

    @Override
    public Object evaluate(String expression, Object context) throws Exception {
        if (context instanceof com.idataconnect.jdbfdriver.DBF) {
            com.idataconnect.jdbfdriver.DBF dbf = (com.idataconnect.jdbfdriver.DBF) context;
            
            // Try each interpreter in order
            Exception lastEx = null;
            for (XBaseInterpreter interpreter : interpreters) {
                try {
                    return interpreter.evaluate(expression, dbf);
                } catch (Exception e) {
                    lastEx = e;
                    // If simple interpreter fails, it probably means it's a complex expression
                    // and we should keep trying other interpreters
                }
            }
            
            if (lastEx != null) throw lastEx;
        }
        
        throw new Exception("No interpreter found to handle expression: " + expression);
    }
}
