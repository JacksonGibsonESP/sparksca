package com.spark.project;

import jep.Jep;
import jep.JepException;
import jep.NDArray;

import java.util.ArrayList;
import java.util.Arrays;

public class Python2 {
    public static void main(String[] args) throws JepException {

        //Почему-то если раскомментировать, то будет падать

        /*
        try (Jep jep = new Jep()) {
            jep.runScript("add.py");
            int a = 2;
            int b = 3;

            jep.eval(String.format("c = add(%d, %d)", a, b));
            Long ans = (Long) jep.getValue("c");

            System.out.println(ans);

            NDArray x = (NDArray) jep.getValue("x");

            System.out.println(x);
            System.out.println(Arrays.toString(x.getDimensions()));
            System.out.println(x.getData().getClass());
            long[] arr = (long[]) x.getData();
            System.out.println(Arrays.toString(arr));

            SomeObject obj = new SomeObject();

            jep.set("obj", obj);
            jep.eval("obj.setFloatData(x)");

            System.out.println(Arrays.toString(obj.getRawdata()));


            ArrayList list = (ArrayList) jep.getValue("list");
            System.out.println(list);

        }*/

        try (Jep jep = new Jep()) {
            jep.runScript("ml.py");

            NDArray res = (NDArray) jep.getValue("res");
            long[] res_arr = (long[]) res.getData();
            System.out.println(Arrays.toString(res_arr));
        }
    }
}
