package com.zudin;

import org.apache.commons.collections.map.MultiValueMap;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMSource;
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
     * Private constructor for singleton
     */
    private PetriNet() {}

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


    public int getNumOfActivities() {
        Set<String> set = new HashSet<String>();
        for (String[] arr : sequences) {
            Collections.addAll(set, arr);
        }
        return set.size();
    }

    public void addSequence(String sequence) {
        sequences.add(sequence.split(";"));
    }

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
     * Make Petri net safe
     */
    public void makeSafe() {
        checkForSafety();
        checkForRedundance();
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

    private void checkForSafety() {
        for (String[] sequence : sequences) {
            Place errorPlace = checkSequence(sequence);
            while (errorPlace != null) {
                deletePlace(errorPlace);
                errorPlace = checkSequence(sequence);
            }
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

    public DOMSource getXml() throws ParserConfigurationException {
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

        //root elements
        Document doc = docBuilder.newDocument();

        Element rootElement = doc.createElement("document");
        doc.appendChild(rootElement);
        rootElement.appendChild(doc.createElement("id"));
        rootElement.appendChild(doc.createElement("x"));
        rootElement.appendChild(doc.createElement("y"));
        rootElement.appendChild(doc.createElement("label"));

        //subnet global
        rootElement.appendChild(createSubnet(doc));

        rootElement.appendChild(doc.createElement("roles"));
        return new DOMSource(doc);
    }

    private Element createSubnet(Document doc) {
        Element subnet = doc.createElement("subnet");
        subnet.appendChild(doc.createElement("id"));
        Element sx = doc.createElement("x");
        sx.appendChild(doc.createTextNode("0"));
        subnet.appendChild(sx);
        Element sy = doc.createElement("y");
        sy.appendChild(doc.createTextNode("0"));
        subnet.appendChild(sy);
        List<Element> list = getQueue(doc, new HashSet<Place>(){{ add(start); }}, new HashSet<Transition>(), 0);
        for (Element element : list) {
            subnet.appendChild(element);
        }
        subnet.appendChild(doc.createElement("label"));
        return subnet;
    }

    private List<Element> getQueue(Document doc, Set<Place> set, Set<Transition> used, int x) {
        List<Element> elements = new ArrayList<Element>();

        Set<Place> newSet = new HashSet<Place>();
        int y = 0;
        int yy = 0;
        for (Place place : set) {
            elements.add(getXmlPlace(doc, place, x, y++));
            for (Transition transition : place.getOut()) {
                if (!used.contains(transition)) {
                    elements.add(getXmlTransition(doc, transition, x, yy++));
                    used.add(transition);
                    for (Place next : transition.getTo()) {
                        newSet.add(next);
                    }
                    elements.addAll(getXmlArcs(doc, transition));
                }
               //elements.add(getXmlArc(doc, Integer.parseInt(place.getName().split("p")[1]), transition.hashCode()));
            }
        }
        if (newSet.isEmpty()) {
            return elements;
        } else {
            elements.addAll(getQueue(doc, newSet, used, x+1));
            return elements;
        }
    }

    private Element getXmlPlace(Document doc, Place place, int x, int y) {
        Element placeElem = doc.createElement("place");
        Element id = doc.createElement("id");
        id.appendChild(doc.createTextNode(String.valueOf(place.getName().split("p")[1])));
        placeElem.appendChild(id);
        Element xel = doc.createElement("x");
        xel.appendChild(doc.createTextNode(String.valueOf(x * 100)));
        placeElem.appendChild(xel);
        Element yel = doc.createElement("y");
        yel.appendChild(doc.createTextNode(String.valueOf(y * 100)));
        placeElem.appendChild(yel);
        placeElem.appendChild(doc.createElement("label"));
        Element tokens = doc.createElement("tokens");
        if (x == 0) {
            tokens.appendChild(doc.createTextNode("1"));
        } else {
            tokens.appendChild(doc.createTextNode("0"));
        }
        placeElem.appendChild(tokens);
        Element isstatic = doc.createElement("isStatic");
        isstatic.appendChild(doc.createTextNode("false"));
        placeElem.appendChild(isstatic);
        return placeElem;
    }

    private Element getXmlTransition(Document doc, Transition transition, int x, int y) {
        Element transElem = doc.createElement("transition");
        Element id = doc.createElement("id");
        id.appendChild(doc.createTextNode(String.valueOf(transition.hashCode())));
        transElem.appendChild(id);
        Element xel = doc.createElement("x");
        xel.appendChild(doc.createTextNode(String.valueOf(x * 100 + 50)));
        transElem.appendChild(xel);
        Element yel = doc.createElement("y");
        yel.appendChild(doc.createTextNode(String.valueOf(y * 100)));
        transElem.appendChild(yel);
        transElem.appendChild(doc.createElement("label"));
        return transElem;
    }

    private List<Element> getXmlArcs(Document doc, Transition transition){
        List<Element> elements = new ArrayList<Element>();
        for (Place place : transition.getFrom()) {
            elements.add(getXmlArc(doc, Integer.parseInt(place.getName().split("p")[1]), transition.hashCode()));
        }
        for (Place place : transition.getTo()) {
            elements.add(getXmlArc(doc, transition.hashCode(), Integer.parseInt(place.getName().split("p")[1])));
        }
        return elements;
    }

    private Element getXmlArc(Document doc, int xid, int yid){
        Element arc = doc.createElement("arc");
        Element type = doc.createElement("type");
        type.appendChild(doc.createTextNode("regular"));
        arc.appendChild(type);
        Element sid = doc.createElement("sourceId");
        sid.appendChild(doc.createTextNode(String.valueOf(xid)));
        arc.appendChild(sid);
        Element did = doc.createElement("destinationId");
        did.appendChild(doc.createTextNode(String.valueOf(yid)));
        arc.appendChild(did);
        Element mult = doc.createElement("multiplicity");
        mult.appendChild(doc.createTextNode("1"));
        arc.appendChild(mult);
        return arc;
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

    static class Transition {
        private List<Place> from = new ArrayList<Place>();
        private List<Place> to = new ArrayList<Place>();
        private String name;

        public Transition(String name) {
            this.name = name;
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

        public void addTo(Place place) {
            to.add(place);
        }

        public void addFrom(Place place) {
            from.add(place);
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

        public String getName() {
            return name;
        }

        public List<Transition> getIn() {
            return new ArrayList<Transition>(in);
        }

        public List<Transition> getOut() {
            return new ArrayList<Transition>(out);
        }

        public void addIn(Transition transition) {
            in.add(transition);
            transition.addTo(this);
        }

        public void addOut(Transition transition) {
            out.add(transition);
            transition.addFrom(this);
        }

        public boolean include(Place place) {
            return in.containsAll(place.getIn()) && out.containsAll(place.getOut());
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
    }
}
