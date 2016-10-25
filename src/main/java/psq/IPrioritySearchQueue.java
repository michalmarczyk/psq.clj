package psq;

import clojure.lang.IPersistentStack;
import clojure.lang.ISeq;


public interface IPrioritySearchQueue extends IPersistentStack {

    ISeq atMost(Object priority);

    ISeq atMostRange(Object low, Object high, Object priority);

    ISeq reverseAtMost(Object priority);

    ISeq reverseAtMostRange(Object low, Object high, Object priority);
}
