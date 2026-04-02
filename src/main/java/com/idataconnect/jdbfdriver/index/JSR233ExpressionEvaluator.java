package com.idataconnect.jdbfdriver.index;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * JSR-233 based expression evaluator.
 */
public class JSR233ExpressionEvaluator implements ExpressionEvaluator {

    private final ScriptEngine engine;

    public JSR233ExpressionEvaluator() {
        ScriptEngineManager manager = new ScriptEngineManager();
        this.engine = manager.getEngineByName("salinas");
    }

    @Override
    public Object evaluate(String expression, Object context) throws Exception {
        if (engine == null) {
            throw new IllegalStateException("Salinas script engine not found on classpath");
        }
        
        if (context != null) {
            // We use reflection to setup a WorkAreaManager and WorkArea because
            // jdbfdriver cannot depend on salinas-core (circular dependency)
            try {
                ClassLoader cl = engine.getClass().getClassLoader();
                Class<?> wamClass = cl.loadClass("com.idataconnect.salinas.data.WorkAreaManager");
                Class<?> waClass = cl.loadClass("com.idataconnect.salinas.data.WorkArea");
                
                Object wam = wamClass.getConstructor().newInstance();
                Object wa = waClass.getConstructor(String.class, context.getClass().getClassLoader().loadClass("com.idataconnect.jdbfdriver.DBF")).newInstance("TEMP", context);
                
                wamClass.getMethod("use", int.class, waClass).invoke(wam, 1, wa);
                
                engine.getContext().setAttribute("salinasWorkAreaManager", wam, javax.script.ScriptContext.ENGINE_SCOPE);
            } catch (Exception e) {
                // If reflection fails, we might just proceed and hope the fields are already there
            }
        }

        try {
            return engine.eval(expression);
        } catch (ScriptException e) {
            throw new Exception("Error evaluating expression: " + expression, e);
        }
    }

    public boolean isAvailable() {
        return engine != null;
    }
}
