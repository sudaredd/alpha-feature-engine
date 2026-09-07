package com.quant.engine.mcp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Native Spring AI MCP auto-configuration that scans the ApplicationContext for
 * beans with methods annotated with {@link McpTool} or {@link Tool}, and automatically
 * exposes them as MCP tools without requiring manual bean declarations.
 */
@AutoConfiguration
public class McpToolAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(McpToolAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(ToolCallbackProvider.class)
    public ToolCallbackProvider mcpToolCallbackProvider(ApplicationContext applicationContext) {
        List<Object> toolBeans = new ArrayList<>();

        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            try {
                Class<?> beanType = applicationContext.getType(beanName);
                if (beanType != null && hasToolMethods(beanType)) {
                    Object bean = applicationContext.getBean(beanName);
                    toolBeans.add(bean);
                    log.info("Auto-discovered MCP tool bean: {} ({})", beanName, beanType.getSimpleName());
                }
            } catch (Exception _) {
                // Ignore any bean definition resolution errors
            }
        }

        if (toolBeans.isEmpty()) {
            return () -> new ToolCallback[0];
        }

        ToolCallback[] callbacks = ToolCallbacks.from(toolBeans.toArray());
        log.info("Registered {} auto-configured MCP tool callback(s)", callbacks.length);
        return () -> callbacks;
    }

    private boolean hasToolMethods(Class<?> clazz) {
        for (Method method : clazz.getMethods()) {
            if (AnnotatedElementUtils.hasAnnotation(method, Tool.class)) {
                return true;
            }
        }
        return false;
    }
}
