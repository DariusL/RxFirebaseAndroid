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
import java.util.Map;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;
import rx.subscriptions.Subscriptions;

/**
 * Fork of https://gist.github.com/gsoltis/86210e3259dcc6998801
 */
public class RxFirebase {

    public static class FirebaseChildEvent <T> {
        @Retention(RetentionPolicy.SOURCE)
        @IntDef({TYPE_ADD, TYPE_CHANGE, TYPE_MOVE, TYPE_REMOVE})
        public @interface EventType{}

        public static final int TYPE_ADD = 1;
        public static final int TYPE_CHANGE = 2;
        public static final int TYPE_REMOVE = 3;
        public static final int TYPE_MOVE = 4;

        public final T value;
        public final @EventType int eventType;
        public final String prevName;

        public FirebaseChildEvent(T value, @EventType int eventType, String prevName) {
            this.value = value;
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

    public static Observable<FirebaseChildEvent<DataSnapshot>> observeChildren(final Query ref) {
        return Observable.create(new Observable.OnSubscribe<FirebaseChildEvent<DataSnapshot>>() {

            @Override
            public void call(final Subscriber<? super FirebaseChildEvent<DataSnapshot>> subscriber) {
                final ChildEventListener listener = ref.addChildEventListener(new ChildEventListener() {
                    @Override
                    public void onChildAdded(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent<>(dataSnapshot, FirebaseChildEvent.TYPE_ADD, prevName));
                    }

                    @Override
                    public void onChildChanged(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent<>(dataSnapshot, FirebaseChildEvent.TYPE_CHANGE, prevName));
                    }

                    @Override
                    public void onChildRemoved(DataSnapshot dataSnapshot) {
                        subscriber.onNext(new FirebaseChildEvent<>(dataSnapshot, FirebaseChildEvent.TYPE_REMOVE, null));
                    }

                    @Override
                    public void onChildMoved(DataSnapshot dataSnapshot, String prevName) {
                        subscriber.onNext(new FirebaseChildEvent<>(dataSnapshot, FirebaseChildEvent.TYPE_MOVE, prevName));
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

    public static Observable<FirebaseChildEvent<DataSnapshot>> observeChildAdded(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_ADD));
    }

    public static Observable<FirebaseChildEvent<DataSnapshot>> observeChildChanged(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_CHANGE));
    }

    public static Observable<FirebaseChildEvent<DataSnapshot>> observeChildMoved(Query ref) {
        return observeChildren(ref).filter(makeEventFilter(FirebaseChildEvent.TYPE_MOVE));
    }

    public static Observable<FirebaseChildEvent<DataSnapshot>> observeChildRemoved(Query ref) {
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

    public static Observable<DataSnapshot> observeOnce(final Query ref){
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

    public static Observable<Firebase> setValue(Firebase firebase, Object value){
        final BehaviorSubject<Firebase> subject = BehaviorSubject.create();
        firebase.setValue(value, new ObservableCompletionListener(subject));
        return subject;
    }

    public static Observable<Firebase> updateChildren(Firebase firebase, Map<String, Object> children){
        final BehaviorSubject<Firebase> subject = BehaviorSubject.create();
        firebase.updateChildren(children, new ObservableCompletionListener(subject));
        return subject;
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
        }).startWith(firebase.getAuth()).distinctUntilChanged();
    }

    public static Observable<AuthData> authAnonymously(Firebase firebase){
        final BehaviorSubject<AuthData> subject = BehaviorSubject.create();
        firebase.authAnonymously(new ObservableAuthResultHandler(subject));
        return subject;
    }

    public static Observable<AuthData> authWithOAuthToken(Firebase firebase, String provider, String token){
        final BehaviorSubject<AuthData> subject = BehaviorSubject.create();
        firebase.authWithOAuthToken(provider, token, new ObservableAuthResultHandler(subject));
        return subject;
    }

    public static Observable<AuthData> authWithCustomToken(Firebase firebase, String token){
        final BehaviorSubject<AuthData> subject = BehaviorSubject.create();
        firebase.authWithCustomToken(token, new ObservableAuthResultHandler(subject));
        return subject;
    }

    private static class ObservableAuthResultHandler implements Firebase.AuthResultHandler{

        private final Observer<AuthData> observer;

        private ObservableAuthResultHandler(Observer<AuthData> observer) {
            this.observer = observer;
        }

        @Override
        public void onAuthenticated(AuthData authData) {
            observer.onNext(authData);
            observer.onCompleted();
        }

        @Override
        public void onAuthenticationError(FirebaseError firebaseError) {
            observer.onError(new FirebaseException(firebaseError));
        }
    }

    private static class ObservableCompletionListener implements Firebase.CompletionListener{

        private final Observer<Firebase> observer;

        private ObservableCompletionListener(Observer<Firebase> observer) {
            this.observer = observer;
        }

        @Override
        public void onComplete(FirebaseError firebaseError, Firebase firebase) {
            if (firebaseError == null){
                observer.onNext(firebase);
                observer.onCompleted();
            } else {
                observer.onError(new FirebaseException(firebaseError));
            }
        }
    }
}