package lt.dariusl.rxfirebaseandroid;

import android.support.annotation.IntDef;

import com.firebase.client.AuthData;
import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.Query;
import com.firebase.client.ValueEventListener;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

/**
 * Fork of https://gist.github.com/gsoltis/86210e3259dcc6998801
 */
public class RxFirebase {

    public static class FirebaseChildEvent {
        @Retention(RetentionPolicy.SOURCE)
        @IntDef({TYPE_ADD, TYPE_CHANGE, TYPE_MOVE, TYPE_REMOVE})
        public @interface EventType{}

        public static final int TYPE_ADD = 1;
        public static final int TYPE_CHANGE = 2;
        public static final int TYPE_REMOVE = 3;
        public static final int TYPE_MOVE = 4;

        public final DataSnapshot snapshot;
        public final @EventType int eventType;
        public final String prevName;

        public FirebaseChildEvent(DataSnapshot snapshot, @EventType int eventType, String prevName) {
            this.snapshot = snapshot;
            this.eventType = eventType;
            this.prevName = prevName;
        }
    }

    public static class FirebaseException extends RuntimeException {
        private final FirebaseError error;

        public FirebaseException(FirebaseError error) {
            super("Firebase error " + error, error.toException());
            this.error = error;
        }

        public FirebaseError getError() {
            return error;
        }
    }

    public static Observable<FirebaseChildEvent> observeChildren(final Query ref) {
        return Observable.create(new Observable.OnSubscribe<FirebaseChildEvent>() {

            @Override
            public void call(final Subscriber<? super FirebaseChildEvent> subscriber) {
                final ChildEventListener listener = ref.addChildEventListener(new ChildEventListener() {
                    @Override
                    public void onChildAdded(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent(dataSnapshot, FirebaseChildEvent.TYPE_ADD, prevName));
                    }

                    @Override
                    public void onChildChanged(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent(dataSnapshot, FirebaseChildEvent.TYPE_CHANGE, prevName));
                    }

                    @Override
                    public void onChildRemoved(DataSnapshot dataSnapshot) {
                        subscriber.onNext(new FirebaseChildEvent(dataSnapshot, FirebaseChildEvent.TYPE_REMOVE, null));
                    }

                    @Override
                    public void onChildMoved(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent(dataSnapshot, FirebaseChildEvent.TYPE_MOVE, prevName));
                    }

                    @Override
                    public void onCancelled(FirebaseError error) {
                        subscriber.onError(new FirebaseException(error));
                    }
                });

                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        ref.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    private static Func1<FirebaseChildEvent, Boolean> makeEventFilter(final @FirebaseChildEvent.EventType int eventType) {
        return new Func1<FirebaseChildEvent, Boolean>() {

            @Override
            public Boolean call(FirebaseChildEvent firebaseChildEvent) {
                return firebaseChildEvent.eventType == eventType;
            }
        };
    }

    public static Observable<FirebaseChildEvent> observeChildAdded(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_ADD));
    }

    public static Observable<FirebaseChildEvent> observeChildChanged(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_CHANGE));
    }

    public static Observable<FirebaseChildEvent> observeChildMoved(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_MOVE));
    }

    public static Observable<FirebaseChildEvent> observeChildRemoved(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_REMOVE));
    }

    public static Observable<DataSnapshot> observe(final Query ref) {

        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {

            @Override
            public void call(final Subscriber<? super DataSnapshot> subscriber) {
                final ValueEventListener listener = ref.addValueEventListener(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot dataSnapshot) {
                        subscriber.onNext(dataSnapshot);
                    }

                    @Override
                    public void onCancelled(FirebaseError error) {
                        subscriber.onError(new FirebaseException(error));
                    }
                });

                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        ref.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    public static Observable<DataSnapshot> observeSingle(final Query ref){
        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {
            @Override
            public void call(final Subscriber<? super DataSnapshot> subscriber) {
                ref.addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot dataSnapshot) {
                        subscriber.onNext(dataSnapshot);
                        subscriber.onCompleted();
                    }

                    @Override
                    public void onCancelled(FirebaseError firebaseError) {
                        subscriber.onError(new FirebaseException(firebaseError));
                    }
                });
            }
        });
    }

    public static Observable<Void> setValue(final Firebase firebase, final Object value){
        return Observable.create(new Observable.OnSubscribe<Void>() {
            @Override
            public void call(final Subscriber<? super Void> subscriber) {
                firebase.setValue(value, new Firebase.CompletionListener() {
                    @Override
                    public void onComplete(FirebaseError firebaseError, Firebase firebase) {
                        if (subscriber.isUnsubscribed()) {
                            return;
                        }

                        if (firebaseError == null) {
                            subscriber.onNext(null);
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(new FirebaseException(firebaseError));
                        }
                    }
                });
            }
        });
    }

    public static Observable<AuthData> observeAuth(final Firebase firebase){
        return Observable.create(new Observable.OnSubscribe<AuthData>() {
            @Override
            public void call(final Subscriber<? super AuthData> subscriber) {
                final Firebase.AuthStateListener listener = firebase.addAuthStateListener(new Firebase.AuthStateListener() {
                    @Override
                    public void onAuthStateChanged(AuthData authData) {
                        subscriber.onNext(authData);
                    }
                });

                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        firebase.removeAuthStateListener(listener);
                    }
                }));
            }
        }).startWith(firebase.getAuth());
    }
}