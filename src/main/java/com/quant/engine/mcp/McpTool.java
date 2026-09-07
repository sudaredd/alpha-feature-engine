package com.quant.engine.mcp;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Model Context Protocol (MCP) Tool annotation.
 * Meta-annotated with Spring AI {@link Tool} for seamless auto-discovery.
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Tool
public @interface McpTool {

    @AliasFor(annotation = Tool.class, attribute = "name")
    String name() default "";

    @AliasFor(annotation = Tool.class, attribute = "description")
    String description() default "";

    @AliasFor(annotation = Tool.class, attribute = "returnDirect")
    boolean returnDirect() default false;
}
