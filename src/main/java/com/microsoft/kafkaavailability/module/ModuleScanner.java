package com.microsoft.kafkaavailability.module;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class ModuleScanner {
    private final static Logger LOGGER = LoggerFactory.getLogger(ModuleScanner.class);

    /**
     * Purpose of this method is to get all Guice module classes, to pick up drop wizard reporters in dependencies.
     *
     * @return
     */
    public static Set<Module> getModulesFromDependencies() {
        Set<Module> modules = new HashSet<>();
        for(Class<? extends AbstractModule> moduleClass : findAllModuleClasses()) {
            try {
                modules.add(moduleClass.newInstance());
                LOGGER.debug(moduleClass.getName() + " is found and included in injection.");
            } catch (InstantiationException | IllegalAccessException e) {
                //Skip module classes that we can't get an instance for
                LOGGER.error("Failed to get instance for module class: " + moduleClass.getName());
            }
        }

        return modules;
    }

    /**
     * This method scans in "com.microsoft.kafkaavailability.module" package and find all Guice Module classes, it will
     * scan KAT and all KAT's dependencies.
     *
     * @return
     */
    private static Set<Class<? extends AbstractModule>> findAllModuleClasses() {
        Reflections reflections = new Reflections("com.microsoft.kafkaavailability.module");
        Set<Class<? extends AbstractModule>> result = new HashSet<>();

        for (Class<? extends AbstractModule> moduleClass : reflections.getSubTypesOf(AbstractModule.class)) {
            if(!isKnownType(moduleClass)) {
                result.add(moduleClass);
            }
        }

        return result;
    }

    private static boolean isKnownType(Class<? extends AbstractModule> moduleClass) {
        return moduleClass.equals(AppModule.class) || moduleClass.equals(ReportersModule.class) || moduleClass.equals(ThreadsModule.class);
    }
}
