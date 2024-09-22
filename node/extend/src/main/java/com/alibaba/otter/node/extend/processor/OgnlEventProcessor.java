package com.alibaba.otter.node.extend.processor;

import cn.hutool.core.convert.Convert;
import com.alibaba.otter.shared.etl.model.EventData;
import ognl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * @author kqwu@best-inc.com
 * @description
 * @date 2024/9/22
 */
public class OgnlEventProcessor extends AbstractEventProcessor {

    private Object expression;
    private final DefaultMemberAccess memberAccess = new DefaultMemberAccess(true);
    private final DefaultClassResolver defaultClassResolver = new DefaultClassResolver();
    private final DefaultTypeConverter defaultTypeConverter = new DefaultTypeConverter();

    private static Logger log = LoggerFactory.getLogger(OgnlEventProcessor.class);

    public OgnlEventProcessor(){

    }

    public OgnlEventProcessor(String source){
        setExpression(source);
    }

    public void setExpression(String source) {
        try {
            this.expression = Ognl.parseExpression(source);
        } catch (OgnlException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean process(EventData eventData) {
        // 构建一个OgnlContext对象
        OgnlContext context = (OgnlContext) Ognl.createDefaultContext(this, memberAccess, defaultClassResolver, defaultTypeConverter);
        try {
            context.put("event",new RuntimeEventData(eventData));
            Object result = Ognl.getValue(expression, context, context.getRoot());
            return Convert.toBool(result,true);
        } catch (OgnlException e) {
            log.error("ognl表达式执行失败，表达式：{}，even: {}", expression, eventData, e);
        }
        return true;
    }

    public static class DefaultMemberAccess implements MemberAccess {
        private boolean allowPrivateAccess = false;
        private boolean allowProtectedAccess = false;
        private boolean allowPackageProtectedAccess = false;

        public DefaultMemberAccess(boolean allowAllAccess) {
            this(allowAllAccess, allowAllAccess, allowAllAccess);
        }

        public DefaultMemberAccess(boolean allowPrivateAccess, boolean allowProtectedAccess,
                                   boolean allowPackageProtectedAccess) {
            super();
            this.allowPrivateAccess = allowPrivateAccess;
            this.allowProtectedAccess = allowProtectedAccess;
            this.allowPackageProtectedAccess = allowPackageProtectedAccess;
        }

        @Override
        public Object setup(Map context, Object target, Member member, String propertyName) {
            Object result = null;

            if (isAccessible(context, target, member, propertyName)) {
                AccessibleObject accessible = (AccessibleObject) member;

                if (!accessible.isAccessible()) {
                    result = Boolean.TRUE;
                    accessible.setAccessible(true);
                }
            }
            return result;
        }

        @Override
        public void restore(Map context, Object target, Member member, String propertyName, Object state) {
            if (state != null) {
                ((AccessibleObject) member).setAccessible((Boolean) state);
            }
        }

        /**
         * Returns true if the given member is accessible or can be made accessible by this object.
         */
        @Override
        public boolean isAccessible(Map context, Object target, Member member, String propertyName) {
            int modifiers = member.getModifiers();
            if (Modifier.isPublic(modifiers)) {
                return true;
            } else if (Modifier.isPrivate(modifiers)) {
                return this.allowPrivateAccess;
            } else if (Modifier.isProtected(modifiers)) {
                return this.allowProtectedAccess;
            } else {
                return this.allowPackageProtectedAccess;
            }
        }
    }
}
