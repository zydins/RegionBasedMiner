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

    /**
     * Set to all integer arrays, given in List, values a1 and a2 from given index
     * @param list list with integer arrays
     * @param index start index
     * @param a1 first value
     * @param a2 second value
     */
    public static void addToAll(List<int[]> list, int index, int a1, int a2) {
        for (int[] aList : list) {
            aList[index] = a1;
            aList[index + 1] = a2;
        }
    }

    /**
     * Deep copy of list of integer arrays
     * @param original original list
     * @return clone list
     */
    public static List<int[]> deepCopy(List<int[]> original) {
        List<int[]> clone = new ArrayList<int[]>();
        for (int[] arr : original) {
            clone.add(arr.clone());
        }
        return clone;
    }


}
