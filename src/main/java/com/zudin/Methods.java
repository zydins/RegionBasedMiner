package com.zudin;

import java.util.ArrayList;
import java.util.List;

/**
 * Sergey Zudin
 * Date: 15.04.14
 */
public class Methods {

    /**
     * Returns sum of all elements in given integer array
     * @param arr integer array
     * @return sum of elements
     */
    public static int sumOfArrElem(int[] arr) {
        int res = 0;
        for (int i : arr) {
            res += i;
        }
        return res;
    }

    public static void addToAll(ArrayList<int[]> list, int index, int a1, int a2) {
        for (int[] aList : list) {
            aList[index] = a1;
            aList[index + 1] = a2;
        }
    }

    public static ArrayList<int[]> deepCopy(List<int[]> original) {
        ArrayList<int[]> clone = new ArrayList<int[]>();
        for (int[] arr : original) {
            clone.add(arr.clone());
        }
        return clone;
    }

//    public static boolean isInner(int[] inner, int[] outer) {
//        for (int i = 0; i < inner.length; i++) {
//            if (inner[i] == 1 && outer[i] == 0) {
//                return false;
//            }
//        }
//        return true;
//    }
}
