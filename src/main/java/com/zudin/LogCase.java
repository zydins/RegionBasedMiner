package com.zudin;

import java.util.*;

/**
 * Sergey Zudin
 * Date: 15.04.14
 */
public class LogCase{
    private static int idCounter = 0;

    private List<int[]> solutions;
    private List<String> activities;
    private Set<LogCase> parents = new HashSet<LogCase>();
    private int id;

    public LogCase(int id, String activities/*, int solutions*/) {
        this.id = id;
        if (id > idCounter) idCounter = id;
        this.activities = transformActivities(activities);
        int num = activities.split(";").length;
        setSolutions(getSolutions(num));
    }

    public LogCase(List<String> activities, List<int[]> solutions, Set<LogCase> parents) {
        this.id = getNewId();
        this.activities = new ArrayList<String>(activities);
        setSolutions(solutions);
        this.parents.addAll(parents);
    }

    @Override
    public String toString() {
        String result = "ID: " + id + ":\n[ ";
        for (String act : activities) {
            result += act + " ";
        }
        result += "]\n";
        for (int[] arr : getSolutions()) {
            result += "[ ";
            for (int i : arr) {
                result += i + "  ";
            }
            result += "]\n";
        }
        return result;
    }

    public void setSolutions(List<int[]> list) {
        solutions = Methods.deepCopy(list);
    }

    public List<int[]> getSolutions() {
        return Methods.deepCopy(solutions);
    }

    public List<String> getActivities() {
        return new ArrayList<String>(activities);
    }

    public Set<LogCase> getParents() {
        return new HashSet<LogCase>(parents);
    }

    public int getId() {
        return id;
    }

    private static int getNewId() {
        return ++idCounter;
    }

    public static List<String> transformActivities(String activities) {
        List<String> newActivities = new ArrayList<String>();
        String[] arr = activities.split(";");
        newActivities.add("c");
        for (int i = 0; i < arr.length; i++) {
            newActivities.add("y" + arr[i]);
            if (i != arr.length - 1) {
                newActivities.add("x" + arr[i]);
            }
        }
        return newActivities;
    }

    public LogCase mergeSolutions(LogCase anotherCase) {
        //1. Find indexes of equal activities
        if (this.activities.equals(anotherCase.activities)) {
            return this;
        }
        List<ArrayList<Integer>> equalActivitiesIndexes = new ArrayList<ArrayList<Integer>>();
        equalActivitiesIndexes.add(new ArrayList<Integer>());
        equalActivitiesIndexes.add(new ArrayList<Integer>());
        List<String> equalActivitiesNames = new ArrayList<String>();
        for (int i = 0; i < this.activities.size(); i++) {
            for (int j = 0; j < anotherCase.activities.size(); j++) {
                if (this.activities.get(i).equals(anotherCase.activities.get(j))) { //if same activity is found
                    equalActivitiesIndexes.get(0).add(i);
                    equalActivitiesIndexes.get(1).add(j);
                    equalActivitiesNames.add(this.activities.get(i));
                }
            }
        }
        List<String> notEqualActivities = new ArrayList<String>(anotherCase.activities);
        notEqualActivities.removeAll(equalActivitiesNames);
        //2.Find unique combinations and create merged solutions
        List<int[]> merdgedSolutions = new ArrayList<int[]>();
        for (int i = 0; i < this.solutions.size(); i++) {
            int[] combination = new int[equalActivitiesIndexes.get(0).size()]; //combination in this case
            for (int j = 0; j < equalActivitiesIndexes.get(0).size(); j++) {
                combination[j] = this.solutions.get(i)[equalActivitiesIndexes.get(0).get(j)];
            }
            for (int n = 0; n < anotherCase.solutions.size(); n++) {
                int[] combination2 = new int[equalActivitiesIndexes.get(1).size()]; //combination in another case
                for (int j = 0; j < equalActivitiesIndexes.get(1).size(); j++) {
                    combination2[j] = anotherCase.solutions.get(n)[equalActivitiesIndexes.get(1).get(j)];
                }
                if (!Arrays.equals(combination, combination2)) {
                    continue;
                }
                //equal combinations found
                int[] newSolution = new int[this.solutions.get(i).length + anotherCase.solutions.get(n).length - combination2.length];
                int m;
                for (m = 0; m < this.solutions.get(i).length; m++) {
                    newSolution[m] =  this.solutions.get(i)[m];
                }
                for (String at : notEqualActivities) {
                    int index =  anotherCase.activities.indexOf(at);
                    newSolution[m++] = anotherCase.solutions.get(n)[index];
                }
                merdgedSolutions.add(newSolution);
            }
        }
        //3. Create new list of activities
        List<String> newActivities = new ArrayList<String>();
        Set<LogCase> newParents = new HashSet<LogCase>();
        if (this.parents.isEmpty()) {
            newParents.add(this);
        } else {
            newParents.addAll(this.parents);
        }
        if (anotherCase.parents.isEmpty()) {
            newParents.add(anotherCase);
        } else {
            newParents.addAll(anotherCase.parents);
        }
        int m;
        for (m = 0; m < this.activities.size(); m++) {
            newActivities.add(this.activities.get(m));
        }
        for (int l = 0; l < notEqualActivities.size(); l++) {
            newActivities.add(notEqualActivities.get(l));
        }
        return new LogCase(newActivities, merdgedSolutions, newParents/*, this, anotherCase*/);
    }

    public static List<String> getFinalPlace(List<LogCase> cases) {
        Set<String> lastPlace = new HashSet<String>();
        for (LogCase logCase : cases) {
            lastPlace.add("x" + logCase.getActivities().get(logCase.getActivities().size() - 1).substring(1));
        }
        return new ArrayList<String>(lastPlace);
    }

    public static List<String> getStartPlace(List<LogCase> cases) {
        Set<String> startPlace = new HashSet<String>();
        startPlace.add("c");
        for (LogCase logCase : cases) {
            startPlace.add(logCase.getActivities().get(1));
        }
        return new ArrayList<String>(startPlace);
    }

    /**
     * Method returns an array with all possible solutions for
     * given system of inequalities.
     * @param variables amount of variables in log
     * @return list contains all possible solutions (through arrays)
     */
    public static ArrayList<int[]> getSolutions(int variables) {
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
                if (Methods.sumOfArrElem(solution) == 1) bound = -1;
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
                        Methods.addToAll(tempList, i * 2, 1, 0);
                        break;
                    case -1:
                        Methods.addToAll(tempList, i * 2, 0, 1);
                        break;
                    case 0:
                        List<int[]> anotherTempList = Methods.deepCopy(tempList);
                        Methods.addToAll(tempList, i * 2, 0, 0);
                        Methods.addToAll(anotherTempList, i * 2, 1, 1);
                        tempList.addAll(anotherTempList);
                        break;
                }
            }
            variablesValues.addAll(tempList);
        }
        return variablesValues;
    }


}
