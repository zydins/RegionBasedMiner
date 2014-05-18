package com.zudin;

import org.apache.commons.collections.map.MultiValueMap;

import java.util.*;

/**
 * @author Sergey Zudin
 * @since 07.05.14
 */
public class PetriNet {
    private static PetriNet instance;
    private static int nameCounter = 1;

    private List<Place> places = new ArrayList<Place>();
    private List<Transition> transitions = new ArrayList<Transition>();
    private Place start;
    private List<String[]> sequences = new ArrayList<String[]>();

    /**
     * Method, that returns the only instance of PetriNet class
     * @return instance of PetriNet class
     */
    static public PetriNet getInstance() {
        if (instance == null) {
            instance = new PetriNet();
        }
        return instance;
    }

    /**
     * Private constructor for singleton
     */
    private PetriNet() {}


    /**
     * Adds new place in Petri net
     * @param activities activities using in place as list of strings
     */
    public void addPlace(List<String> activities) {
        //одна запись описывает новое место!!!, переходы могут повторяться
        Place place = new Place();
        for (String activity : activities) {
            Transition transition = findTransition(activity.substring(1));
            switch (activity.charAt(0)) {
                case 'x':
                    place.addIn(transition);
                    break;
                case 'y':
                    place.addOut(transition);
                    break;
                case 'c': //это стартовое место
                    start = place;
                    break;
            }
        }
        places.add(place);
    }

    /**
     * Make Petri net safe
     */
    public void makeSafe() {
        //checkForValid();
//        checkForCycles();
//        Place errorPlace = checkSequence();
//        while (errorPlace != null) {
//            //нужна проверка на пустой переход
////            boolean connectedWithStart = false;
////            for (Transition transition : start.getOut()) {
////                if (errorPlace.getIn().contains(transition)) {
////                    transition.removePlace(errorPlace);
////                    errorPlace.removeTransition(transition);
////                    connectedWithStart = true;
////                }
////            }
////            if (!connectedWithStart) {
//                deletePlace(errorPlace);
////            }
//            errorPlace = checkSequence();
//        }
        checkForSafety();
        checkForRedundance();
    }

    private void checkForSafety() {
//        List<PetriNet> variants = new ArrayList<PetriNet>();
        for (String[] sequence : sequences) {
//            PetriNet net = new PetriNet(this);
            Place errorPlace = checkSequence(sequence);
            while (errorPlace != null) {
                deletePlace(errorPlace);
                errorPlace = checkSequence(sequence);
            }
//            variants.add(net);
        }
    }

    /**
     * Check safety of Petri net
     * @return Place where error is raised or null if net is safe
     */
    private Place checkSequence(String[] sequence) { //добавляются только те места, у которого все предыдущие уже есть в очереди
        Set<Place> hasTokens = new HashSet<Place>(); //place has tokens
        hasTokens.add(start);
        List<Transition> were = new ArrayList<Transition>();    //циклы прверить
        List<Transition> toExecute = getTransitionsToExecute(hasTokens);
        int i = 0;
        while (!toExecute.isEmpty() && i < sequence.length) {
            Set<Place> tempSet = new HashSet<Place>();
            toExecute = getTransitionsToExecute(hasTokens);
            Transition choosen = null;
            for (Transition transition : toExecute) {
                if (transition.getName().equals(sequence[i])) {
                    choosen = transition;
                    break;
                }
            }
            if (choosen == null) {
                //???
            }
            hasTokens.removeAll(choosen.getFrom());
            for (Place place : choosen.getTo()) {
                if (hasTokens.contains(place)) {
                    return place;
                } else {
                    tempSet.add(place);
                }
            }
            were.add(choosen);
            hasTokens.addAll(tempSet);
            i++;
        }
        return null;
    }

    private void checkForRedundance() {
        for (Transition transition : transitions) {
            List<Transition> prevTransitions = new ArrayList<Transition>();
            MultiValueMap map = new MultiValueMap();
            Set<Transition> suspicious = new HashSet<Transition>();
            List<Place> froms = transition.getFrom();
            for (Place place : froms) {
                List<Transition> ins = place.getIn();
                for (Transition in : ins) {
                    map.put(in, place);
                    if (prevTransitions.contains(in)) { //данный переход уже БЫЛ, надо добавить все места между этими переходами
                        suspicious.add(in);
                    } else {
                        prevTransitions.add(in);
                    }
                }
            }
            for (Transition suspect : suspicious) {
                List<Place> placeList = new ArrayList<Place>();
                for (Object obj : map.getCollection(suspect)) {  //добавить проверку на последний
                    Place place = (Place) obj;
                    if (place.getOut().size() == 1 && place.getIn().size() == 1) {
                        deletePlace(place);
                    } else {
                        placeList.add(place);
                    }
                }
                Set<Integer> deleteIndexes = new HashSet<Integer>();
                if (placeList.size() > 1) {
                    for (int i = 0; i < placeList.size(); i++) {
                        for (int j = 0; j < placeList.size() && i != j; j++) {
                            if (placeList.get(i).include(placeList.get(j))) {  //проверитть входит ли кто еще!!!
                                //проверить если такой лог
                                //deletePlace(placeList.get(i));
                                //break;
                                deleteIndexes.add(j);
                            }
                        }
                    }
                }
                for (int index : deleteIndexes) {
                    deletePlace(placeList.get(index));
                }
            }
        }
//        if (transitions.size() > 7) { //слишком много переходов из старта, надо убрать
//            Set<String> names = new HashSet<String>();
//            for (String[] sequence : sequences) {
//                names.add(sequence[1]); //добавляем только корректные переходы
//            }
//            List<Transition> startTrans = start.getOut();
//            for (Transition transition : startTrans) {
//                List<Place> listPlace = transition.getTo();
//                for (Place place : listPlace) {
//                    List<Transition> nextTrans = place.getOut();
//                    for (Transition next : nextTrans) {
//                        if (!names.contains(next.getName())) {
//                            if (place.getOut().size() == 1 && place.getIn().size() == 1) {
//                                deletePlace(place);
//                            }
////                            else {
////                            place.removeTransition(transition);
////                            transition.removePlace(place);
//                        }
//                    }
//                }
//            }
//        }
    }

    private List<Transition> getTransitionsToExecute(Set<Place> set) {
        Set<Transition> toExecute = new HashSet<Transition>();
        for (Place place : set) {
            List<Transition> transitionList = place.getOut();
            for (Transition transition : transitionList) {
                if (transition.isAvailable(set)) {
                    toExecute.add(transition);
                }
            }
        }
        return new ArrayList<Transition>(toExecute);
    }

    private void deletePlace(Place place) {
        for (Transition transition : place.getIn()) {
            transition.removePlace(place);
        }
        for (Transition transition : place.getOut()) {
            transition.removePlace(place);
        }
        places.remove(place);
    }

    /**
     * Finds transition with name given as parameter in the list with all transitions.
     * If there isn't created transitions, creates it and adds to the list with transitions.
     * @param name name of transitions that have to be found
     * @return transition with given name
     */
    private Transition findTransition(String name) {
        if (name.isEmpty()) return null;
        for (Transition transition : transitions) {
            if (transition.getName().equals(name)) {
                return transition;
            }
        }
        Transition transition = new PetriNet.Transition(name);
        transitions.add(transition);
        return transition;
    }

    public void addSequence(String sequence) {
        sequences.add(sequence.split(";"));
    }

    public int getNumOfActivities() {
        Set<String> set = new HashSet<String>();
        for (String[] arr : sequences) {
            Collections.addAll(set, arr);
        }
        return set.size();
    }

    @Override
    public String toString() {
        String res = "";
        for (Place place : places) {
            res += place.toString() + "\n";
        }
        for (Transition transition : transitions) {
            res += transition.toString() + "\n";
        }
        return res;
    }
//
//    @Override
//    public Object clone() {
//
//    }

    static class Transition{
        private List<Place> from = new ArrayList<Place>();
        private List<Place> to = new ArrayList<Place>();
        private String name;

        public Transition(String name) {
            this.name = name;
        }

        public void addFrom(Place place) {
            from.add(place);
        }

        public void addTo(Place place) {
            to.add(place);
        }

        public String getName() {
            return name;
        }

        public List<Place> getTo() {
            return new ArrayList<Place>(to);
        }

        public List<Place> getFrom() {
            return new ArrayList<Place>(from);
        }


        public boolean removePlace(Place toDelete) {
            return from.remove(toDelete) || to.remove(toDelete);
        }

        public boolean isAvailable(Set<Place> query) {
            List<Place> prev = new ArrayList<Place>(from);
            prev.removeAll(query);
            return prev.isEmpty();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Transition)) {
                return false;
            }
            Transition other = (Transition) obj;
            return name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        @Override
        public String toString() {
            String res = "Transition " + name + "; from ";
            for (Place place : from) {
                res += place.getName() + " ";
            }
            res += ", to ";
            for (Place place : to) {
                res += place.getName() + " ";
            }
            return res;
        }
    }

    static class Place {
        private List<Transition> in = new ArrayList<Transition>();
        private List<Transition> out = new ArrayList<Transition>();
        private String name;
        public Place() {
            name = "p" + nameCounter++;
        }

        public void addIn(Transition transition) {
            in.add(transition);
            transition.addTo(this);
        }

        public void addOut(Transition transition) {
            out.add(transition);
            transition.addFrom(this);
        }

        public List<Transition> getIn() {
            return new ArrayList<Transition>(in);
        }

        public List<Transition> getOut() {
            return new ArrayList<Transition>(out);
        }

        @Override
        public String toString() {
            StringBuilder res = new StringBuilder();
            res.append("Place ");
            res.append(name);
            res.append("; from ");
            for (Transition tran : in) {
                res.append(tran.getName());
                res.append(" ");
            }
            res.append(", to ");
            for (Transition tran : out) {
                res.append(tran.getName());
                res.append(" ");
            }
            return res.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Place)) {
                return false;
            }
            Place other = (Place) obj;
            return name.equals(other.name);
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }

        public boolean removeTransition(Transition toDelete) {
            return in.remove(toDelete) || out.remove(toDelete);
        }

        public String getName() {
            return name;
        }

        public boolean include(Place place) {
            return in.containsAll(place.getIn()) && out.containsAll(place.getOut());
        }
    }
}
