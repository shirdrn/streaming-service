package org.shirdrn.streaming.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ReflectionUtils {

	private static final ClassLoader DEFAULT_CLASSLOADER = ReflectionUtils.class.getClassLoader();

	// New Instance Methods
	
	public static Object newInstance(String className) {
		return newInstance(className, DEFAULT_CLASSLOADER);
	}

	public static Object newInstance(String className, ClassLoader classLoader) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true, getClassLoader(classLoader));
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}

	public static <T> T newInstance(String className, Class<T> baseClass, ClassLoader classLoader) {
		return newInstance(className, baseClass, classLoader, new Object[] {});
	}

	public static <T> T newInstance(String className, Class<T> baseClass, ClassLoader classLoader, Object... parameters) {
		T instance = null;
		try {
			Class<T> clazz = newClass(className, baseClass, classLoader);
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	public static Object newInstance(String className, Object... parameters) {
		return newInstance(className, getClassLoader(null), parameters);
	}

	public static Object newInstance(String className, ClassLoader classLoader, Object... parameters) {
		return newInstance(className, classLoader, parameters);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T newInstance(String className, ClassLoader classLoader, Class<T> baseClass, Object... parameters) {
		T instance = null;
		try {
			Class<?> clazz = Class.forName(className, true, getClassLoader(classLoader));
			instance = (T) newInstance(clazz, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	
	

	public static <T> T newInstance(Class<T> clazz, Object... parameters) {
		T instance = null;
		try {
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}

	@SuppressWarnings("unchecked")
	private static <T> T construct(Class<T> clazz, Object... parameters)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		T instance = null;
		Constructor<T>[] constructors = (Constructor<T>[]) clazz.getConstructors();
		for (Constructor<T> c : constructors) {
			if (c.getParameterTypes().length == parameters.length) {
				instance = c.newInstance(parameters);
				break;
			}
		}
		return instance;
	}

	public static <T> T getInstance(Class<T> clazz) {
		return createInstance(clazz);
	}

	private static <T> T createInstance(Class<T> clazz) {
		T instance = null;
		try {
			instance = clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return instance;
	}
	
	
	// New Class Object

	public static <T> Class<T> newClass(String className, Class<T> baseClass) {
		return newClass(className, baseClass, null);
	}

	public static <T> Class<T> newClass(String className, Class<T> baseClass, ClassLoader classLoader) {
		return newClazz(className, classLoader, baseClass);
	}

	@SuppressWarnings("unchecked")
	private static <T> Class<T> newClazz(String className, ClassLoader classLoader, Class<T> baseClass) {
		Class<T> clazz = null;
		try {
			clazz = (Class<T>) Class.forName(className, true, getClassLoader(classLoader));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return clazz;
	}

	public static Object newClass(String className, ClassLoader classLoader) {
		Object clazz = null;
		try {
			clazz = Class.forName(className, true, getClassLoader(classLoader));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return clazz;
	}
	
	private static ClassLoader getClassLoader(ClassLoader classLoader) {
		if (classLoader == null) {
			classLoader = DEFAULT_CLASSLOADER;
		}
		return classLoader;
	}
}