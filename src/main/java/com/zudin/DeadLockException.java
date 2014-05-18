package com.zudin;


/**
 * @author Sergey Zudin
 * @since 14.05.14
 */
public class DeadLockException extends Exception {
    public DeadLockException(PetriNet.Transition transition) {
        super("Dead Lock is found in the " + transition.getName());
    }
}
