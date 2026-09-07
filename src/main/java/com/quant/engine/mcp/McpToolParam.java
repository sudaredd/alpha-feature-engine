package com.quant.engine.mcp;

import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Model Context Protocol (MCP) Tool Parameter annotation.
 * Meta-annotated with Spring AI {@link ToolParam} for seamless auto-discovery.
 */
@Target({ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ToolParam
public @interface McpToolParam {

    @AliasFor(annotation = ToolParam.class, attribute = "description")
    String description() default "";

    @AliasFor(annotation = ToolParam.class, attribute = "required")
    boolean required() default true;
}
