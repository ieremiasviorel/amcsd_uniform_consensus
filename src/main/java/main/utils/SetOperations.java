package main.utils;

import java.util.HashSet;
import java.util.Set;

public class SetOperations {

    public static <T> Set<T> intersection(Set<T> setA, Set<T> setB) {
        Set<T> intersection = new HashSet<T>(setA);
        intersection.retainAll(setB);

        return intersection;
    }

    public static <T> boolean belongs(T item, Set<T> itemSet) {
        return itemSet.contains(item);
    }

    public static <T> Set<T> remove(Set<T> itemSet, T item) {
        Set<T> difference = new HashSet<>(itemSet);
        difference.remove(item);

        return difference;
    }

    public static <T> Set<T> difference(Set<T> minuend, Set<T> subtrahend) {
        Set<T> difference = new HashSet<>(minuend);
        difference.removeAll(subtrahend);

        return difference;
    }
}
