package com.acacia.angleddream.common;


import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;

import java.util.Properties;

public class JythonFactory {

    private static JythonFactory instance = null;

    public synchronized static JythonFactory getInstance(){
        if(instance == null){
            instance = new JythonFactory();
        }

        return instance;

    }

    public static Object getJythonObject(String interfaceName,
                                         String pathToJythonModule){

        Object javaInt = null;
        PythonInterpreter interpreter = new PythonInterpreter();
        Properties props = new Properties();
        props.setProperty("python.path", "/usr/share/jython:/home/bradford/pypipes-dev/Lib:/home/bradford/pypipes-dev");
        props.setProperty("sys.path", "/usr/share/jython:/home/bradford/pypipes-dev/Lib:/home/bradford/pypipes-dev");

        interpreter.initialize(System.getProperties(), props,new String[] {""});
        PySystemState ps = interpreter.getSystemState();
        ps.path.append(new PyString("/usr/share/jython/Lib"));
        ps.path.append(new PyString("/usr/share/jython/"));
        ps.path.append(new PyString("/home/bradford/pypipes-dev/Lib"));
        ps.prefix = new PyString("/usr/share/jython/");


        interpreter.execfile(pathToJythonModule);
        String tempName = pathToJythonModule.substring(pathToJythonModule.lastIndexOf("/")+1);
        tempName = tempName.substring(0, tempName.indexOf("."));
        System.out.println(tempName);
        String instanceName = tempName.toLowerCase();
        String javaClassName = tempName.substring(0,1).toUpperCase() +
                tempName.substring(1);
        String objectDef = "=" + javaClassName + "()";
        interpreter.exec(instanceName + objectDef);
        try {
            Class JavaInterface = Class.forName(interfaceName);
            javaInt = interpreter.get(instanceName).__tojava__(JavaInterface);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();  // Add logging here
        }

        return javaInt;
    }

}
