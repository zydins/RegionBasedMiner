package com.zudin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;

/**
 * Sergey Zudin
 * Date: 30.03.14
 */
public class TestClass {
    public static void main(String... args) {
//        int[][] array = new int[2][];
//
//        int num = 2; //amount of systems
//        int k = 3; //amount of pairs in each system
//        ArrayList<int[]> res = getSolutions(3);
//        for (int[] a : res) {
//            System.out.println("[" + a[0] + ", " + a[1] + ", " + a[2] + ", " + a[3] + ", " + a[4] + ", " + a[5] + "]");
//        }
        //ArrayList<String> list = new ArrayList<String>(){{ add("1"); add("2"); }};
        System.out.println(Integer.parseInt("p1".split("p")[0]));
    }

    /**
     * Method returns an array with all possible solutions for
     * given system of inequalities.
     * @param variables amount of variables in log
     * @return list contains all possible solutions (through arrays)
     */
    private static ArrayList<int[]> getSolutions(int variables) {
        //at first, will find all possible values of variables pairs
        ArrayList<int[]> pairsValues = new ArrayList<int[]>();
        pairsValues.add(new int[]{0});
        pairsValues.add(new int[]{1});
        ArrayList<int[]> tempList = new ArrayList<int[]>();
        for (int i = 1; i < variables; i++) {
            tempList.clear();
            //add values to existing arrays
            for (int[] solution : pairsValues) {
                int bound = 0;
                if (sum(solution) == 1) bound = -1;
                for (int k = 1; k >= bound; k--) {
                    int[] tempArray = Arrays.copyOf(solution, solution.length + 1);
                    tempArray[solution.length] = k;
                    tempList.add(tempArray);
                }
            }
            pairsValues = new ArrayList<int[]>(tempList);
        }

        //now result contains all possible combinations of pairs, will parse it to variables
        int len = variables * 2;
        ArrayList<int[]> variablesValues = new ArrayList<int[]>();

        for (int[] pairValues : pairsValues) {
            tempList.clear();
            tempList.add(new int[len]);
            for (int i = 0; i < pairValues.length; i++) {
                switch (pairValues[i]) {
                    case 1:
                        addToAll(tempList, i*2, 1, 0);
                        break;
                    case -1:
                        addToAll(tempList, i*2, 0, 1);
                        break;
                    case 0:
                        ArrayList<int[]> anotherTempList = deepCopy(tempList);
                        addToAll(tempList, i*2, 0, 0);
                        addToAll(anotherTempList, i*2, 1, 1);
                        tempList.addAll(anotherTempList);
                        break;
                }
            }
            variablesValues.addAll(tempList);
        }
        return variablesValues;
    }

    /**
     * Returns sum of all elements in given integer array
     * @param arr integer array
     * @return sum of elements
     */
    private static int sum(int[] arr) {
        int res = 0;
        for (int i : arr) {
            res += i;
        }
        return res;
    }

    private static void addToAll(ArrayList<int[]> list, int index, int a1, int a2) {
        for (int i = 0; i < list.size(); i++) {
            list.get(i)[index] = a1;
            list.get(i)[index + 1] = a2;
        }
    }

    private static ArrayList<int[]> deepCopy(ArrayList<int[]> original) {
        ArrayList<int[]> clone = new ArrayList<int[]>();
        for (int[] arr : original) {
            clone.add(arr.clone());
        }
        return clone;
    }
}
