package com.alleyz.tsp.producer;

/**
 * Created by alleyz on 2017/5/26.
 */
public class App {

    public static void main(String[] args) {
        String file = "file:/home/jstorm/jstorm-2.2.1/data/supervisor/stormdist/record-topology-13-1495781351/stormjar.jar!/nlpir";
        file = file.substring(file.indexOf(":") + 1, file.indexOf("!"));
        System.out.println(file);
        System.out.println(file.substring(0, file.lastIndexOf("/") + 1));
    }
}
