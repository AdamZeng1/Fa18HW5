package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 */
public class LockManager {
    // These members are given as a suggestion. You are not required to use them, and may
    // delete them and add members as you see fit.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    private Map<ResourceName, List<Pair<Long, Lock>>> resourceLocks = new HashMap<>();
    private Deque<LockRequest> waitingQueue = new ArrayDeque<>();

    // You should not modify this.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    public LockManager() {}

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public LockContext databaseContext() {
        if (!contexts.containsKey("database")) {
            contexts.put("database", new LockContext(this, null, "database"));
        }
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock. No error checking is performed for holding
     * requisite parent locks or freeing dependent child locks. Blocks the transaction and
     * places it in queue if the requested lock is not compatible with another transaction's
     * lock on the resource. Unblocks and unqueues all transactions that can be unblocked
     * after releasing locks in RELEASELOCKS, in order of lock request.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        //TODO needs more work on acuquiring
        //acquire
        Lock lockOnName = new Lock(name, lockType);
        boolean sameTransID = false;
        boolean blocked = false;
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            //Throws exception of a lock on NAME is held by TRANSACTION
            if (transaction.getTransNum() == key) {
                sameTransID = true;
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        if (!releaseLocks.contains(name)) {
                            throw new DuplicateLockRequestException("a lock on " + name
                                    + " is held by " + transaction);
                        }
                    }
                }
                //Blocks the transaction and places it in queue if the requested
                //lock is not compatible with another transaction's lock on the
                //resource
            } else {
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        if (!LockType.compatible(value.get(i).lockType, lockType)) {
                            transaction.block();
                            System.out.println(transaction + " is blocked"); //debug
                            LockRequest newRequest = new LockRequest(transaction, lockOnName);
                            this.waitingQueue.addLast(newRequest);
                            blocked = true;
                            System.out.println("a lock on " + name + " held by " + transaction +
                                    " is added to the queue"); //debug
                        }
                    }
                }
            }
        }

        if (!blocked) {
            if (sameTransID) {
                this.transactionLocks.get(transaction.getTransNum()).add(lockOnName);
                System.out.println("A lock on " + name + " held by " + transaction +
                        " is added to transactionLocks"); //debug
            } else {
                List<Lock> list = new ArrayList<>();
                list.add(lockOnName);
                this.transactionLocks.put(transaction.getTransNum(), list);
                System.out.println("A lock on " + name + " held by " + transaction +
                        " is added to transactionLocks"); //debug
            }
        }

        //Releasing all locks in releaseLocks
        for (int i = 0; i < releaseLocks.size(); i++) {
            if (this.transactionLocks.containsKey(transaction.getTransNum()) && !transaction.getBlocked()) {
                boolean lockOnNameHeld = false;
                List<Lock> list = this.transactionLocks.get(transaction.getTransNum());
                for (int j = 0; j < list.size(); j++) {
                    if (list.get(j).name.equals(releaseLocks.get(i))) {
                        lockOnNameHeld = true;
                        this.transactionLocks.get(transaction.getTransNum()).remove(j);
                        System.out.println("A lock on " + name + " held by " + transaction + " is removed"); //debug
                    }
                }
                if (!lockOnNameHeld) {
                    throw new NoLockHeldException("no lock on " + name + " is held by " + transaction);
                }
            }
        }

        //Unblock and unqueue
        Iterator iterator = this.waitingQueue.iterator();
        while (iterator.hasNext()) {
            LockRequest lockRequest = (LockRequest) iterator.next();
            //check if queue can be unblock and unqueue
            boolean notCompatible = false;
            for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
                Long key = entry.getKey();
                List<Lock> value = entry.getValue();
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(lockRequest.lock.name)) {
                        if (!LockType.compatible(value.get(i).lockType, lockRequest.lock.lockType)) {
                            notCompatible = true;
                        }
                    }
                }
            }

            if (!notCompatible) {
                lockRequest.transaction.unblock();
                System.out.println(lockRequest.transaction + " is unblocked"); //debug
                System.out.println("Unqueueing " + lockRequest.transaction + " with " +
                        lockRequest.lock.name + " in progress ..."); //debug
                acquire(lockRequest.transaction, lockRequest.lock.name, lockRequest.lock.lockType);
                iterator.remove();
                System.out.println("Finish unqueue " + lockRequest.transaction + " with " +
                        lockRequest.lock.name); //debug
            }
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION. No error
     * checking is performed for holding requisite parent locks. Blocks the
     * transaction and places it in queue if the requested lock is not compatible
     * with another transaction's lock on the resource.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        Lock lockOnName = new Lock(name, lockType);
        boolean sameTransID = false;
        boolean blocked = false;
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            //Throws exception of a lock on NAME is held by TRANSACTION
            if (transaction.getTransNum() == key) {
                sameTransID = true;
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        throw new DuplicateLockRequestException("a lock on " + name
                                + " is held by " + transaction);
                    }
                }
                //Blocks the transaction and places it in queue if the requested
                //lock is not compatible with another transaction's lock on the
                //resource
            } else {
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        if (!LockType.compatible(value.get(i).lockType, lockType)) {
                            transaction.block();
                            System.out.println(transaction + " is blocked"); //debug
                            LockRequest newRequest = new LockRequest(transaction, lockOnName);
                            this.waitingQueue.addLast(newRequest);
                            blocked = true;
                            System.out.println("a lock on " + name + " held by " + transaction +
                                    " is added to the queue"); //debug
                        }
                    }
                }
            }
        }

        if (!blocked) {
            if (sameTransID) {
                this.transactionLocks.get(transaction.getTransNum()).add(lockOnName);
                System.out.println("A lock on " + name + " held by " + transaction +
                        " is added to transactionLocks"); //debug
            } else {
                List<Lock> list = new ArrayList<>();
                list.add(lockOnName);
                this.transactionLocks.put(transaction.getTransNum(), list);
                System.out.println("A lock on " + name + " held by " + transaction +
                        " is added to transactionLocks"); //debug
            }
        }
    }

    /**
     * Release TRANSACTION's lock on NAME. No error checking is performed for
     * freeing dependent child locks. Unblocks and unqueues all transactions
     * that can be unblocked, in order of lock request.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        if (this.transactionLocks.containsKey(transaction.getTransNum()) && !transaction.getBlocked()) {
            boolean lockOnName = false;
            List<Lock> list = this.transactionLocks.get(transaction.getTransNum());
            for (int i = 0; i < list.size(); i++) {
                //System.out.println(list.get(i).name); //debug
                if (list.get(i).name.equals(name)) {
                    lockOnName = true;
                    this.transactionLocks.get(transaction.getTransNum()).remove(i);
                    System.out.println("A lock on " + name + " held by " + transaction + " is removed"); //debug
                }
            }
            if (!lockOnName) {
                throw new NoLockHeldException("no lock on " + name + " is held by " + transaction);
            }
        }

        Iterator iterator = this.waitingQueue.iterator();
        while (iterator.hasNext()) {
            LockRequest lockRequest = (LockRequest) iterator.next();
            //check if queue can be unblock and unqueue
            boolean notCompatible = false;
            for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
                Long key = entry.getKey();
                List<Lock> value = entry.getValue();
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(lockRequest.lock.name)) {
                        if (!LockType.compatible(value.get(i).lockType, lockRequest.lock.lockType)) {
                            notCompatible = true;
                        }
                    }
                }
            }

            if (!notCompatible) {
                lockRequest.transaction.unblock();
                System.out.println(lockRequest.transaction + " is unblocked"); //debug
                System.out.println("Unqueueing " + lockRequest.transaction + " with " +
                        lockRequest.lock.name + " in progress ..."); //debug
                acquire(lockRequest.transaction, lockRequest.lock.name, lockRequest.lock.lockType);
                iterator.remove();
                System.out.println("Finish unqueue " + lockRequest.transaction + " with " +
                        lockRequest.lock.name); //debug
            }
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE. No error checking is
     * performed for holding requisite locks. Blocks the transaction and places
     * TRANSACTION in the **front** of the queue if the request cannot be
     * immediately granted (i.e. another transaction holds a conflicting lock). A
     * lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        boolean compatible = true;
        boolean hasTransaction = false;
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            if (key == transaction.getTransNum()) {
                hasTransaction = true;
                boolean hasLockOnName = false;
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        hasLockOnName = true;
                        //Check InvalidLockException
                        if (!LockType.substitutable(newLockType, value.get(i).lockType)) {
                            throw new InvalidLockException("the requested lock type is not a promotion");
                        }
                        //Check DuplicateLockRequestException
                        if (value.get(i).lockType.equals(newLockType)) {
                            throw new DuplicateLockRequestException(transaction + " already has a " +
                                    newLockType + " on " + name);
                        }
                    }
                }
                //Check NoLockHeldException
                if (!hasLockOnName) {
                    throw new NoLockHeldException(transaction + " has no lock on " + name);
                }
            } else {
                for (int i = 0; i < value.size(); i++) {
                    if (value.get(i).name.equals(name)) {
                        if (!LockType.compatible(value.get(i).lockType, newLockType)) {
                            compatible = false;
                            transaction.block();
                            System.out.println(transaction + " is blocked"); //debug
                            Lock lockOnName = new Lock(name, newLockType);
                            LockRequest newRequest = new LockRequest(transaction, lockOnName);
                            this.waitingQueue.addFirst(newRequest);
                            System.out.println(transaction + " with " + newLockType + " is added to the " +
                                    "front of queue"); //debug
                        }
                    }
                }
            }
        }
        //Check NoLockHeldException
        if (!hasTransaction) {
            throw new NoLockHeldException(transaction + "does not exists");
        }

        if (compatible) {
            List<Lock> list = this.transactionLocks.get(transaction.getTransNum());
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).name.equals(name)) {
                    LockType oldLockType = list.get(i).lockType;
                    this.transactionLocks.get(transaction.getTransNum()).get(i).lockType = newLockType;
                    System.out.println("promote " + transaction + " lock of " + name + " from " +
                            oldLockType + " to " + newLockType);
                }
            }
        }


        /*if (this.transactionLocks.containsKey(transaction.getTransNum())) {
            List<Lock> list = this.transactionLocks.get(transaction.getTransNum());
            boolean contains = false;
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).name == name) {
                    contains = true;
                    if (list.get(i).lockType == newLockType) {
                        throw new DuplicateLockRequestException("RANSACTION already has a NEWLOCKTYPE lock on NAME");
                    }
                    if (LockType.substitutable(newLockType, list.get(i).lockType)) {
                        this.transactionLocks.get(transaction.getTransNum()).get(i).lockType = newLockType;
                    } else {
                        throw new InvalidLockException("the requested lock type is not a promotion");
                    }
                }
            }
            if (!contains) {
                throw new NoLockHeldException("TRANSACTION has no lock on NAME");
            }
        }*/
    }

    /**
     * Return the type of lock TRANSACTION has on NAME, or null if no lock is
     * held.
     */
    public LockType getLockType(BaseTransaction transaction, ResourceName name) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        if (this.transactionLocks.containsKey(transaction.getTransNum())) {
            //System.out.println("Contains key"); //debug
            List<Lock> locks = this.transactionLocks.get(transaction.getTransNum());
            for (int i = 0; i < locks.size(); i++) {
                if (name == locks.get(i).name) {
                    //System.out.println(locks.get(i).lockType.toString()); //debug
                    return locks.get(i).lockType;
                }
            }
        } else {
            return null;
        }
        return null;
    }

    /**
     * Returns the list of transactions ids and lock types for locks held on
     * NAME, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<Long, LockType>> getLocks(ResourceName name) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        List<Pair<Long, LockType>> list = new ArrayList<>();
//        for (int i = 0; i < this.resourceLocks.get(name).size(); i++) {
//            list.add(new Pair<>(this.resourceLocks.get(name).get(i).getFirst(), this.resourceLocks.get(name).get(i).getSecond().lockType));
//        }
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            for (int i = 0; i < value.size(); i++) {
                if (name == value.get(i).name) {
                    list.add(new Pair<>(key, value.get(i).lockType));
                }
            }
        }
        return list;
    }

    /**
     * Returns the list of resource names and lock types for locks held by
     * TRANSACTION, in order of acquisition. A promotion should count as acquired
     * at the original time.
     */
    public List<Pair<ResourceName, LockType>> getLocks(BaseTransaction transaction) {
        //throw new UnsupportedOperationException("TODO(hw5): implement");
        List<Pair<ResourceName, LockType>> list = new ArrayList<>();
        for (Map.Entry<Long, List<Lock>> entry : this.transactionLocks.entrySet()) {
            Long key = entry.getKey();
            List<Lock> value = entry.getValue();
            if (key == transaction.getTransNum()) {
                for (int i = 0; i < value.size(); i++) {
                    list.add(new Pair<>(value.get(i).name, value.get(i).lockType));
                }
            }
        }
        return list;
    }
}
